package cn.itsub.kynet.registry;

import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.protocol.*;
import cn.itsub.kynet.common.transport.BufferPool;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Registration and directory discovery only. Business frames belong to peer transports. */
public class RegistryServer {
    private static final Logger log = LoggerFactory.getLogger(RegistryServer.class);
    private static final MessageType[] MESSAGE_TYPES = MessageType.values();
    private static final long SESSION_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5);
    static final int MAX_FRAME_BYTES = Math.max(64 * 1024, Integer.getInteger("kynet.registry.maxFrameBytes", 1024 * 1024));
    private static final AttributeKey<RegistrySession> SESSION_KEY = AttributeKey.valueOf("registrySession");
    private final int port;
    private final RegistryNodeConfig nodeConfig;
    private final RegistryClusterRouteTable clusterRouteTable;
    private final RegistryClusterService clusterService;
    private final ScheduledExecutorService maintenanceExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<Integer, RegistrySession> sessionsByInstanceId = new ConcurrentHashMap<>();
    private final Map<RegistrySession, DirectoryDelivery> directoryDeliveries = new ConcurrentHashMap<>();
    private final LongAdder registrations = new LongAdder();
    private final LongAdder rejectedMessages = new LongAdder();
    private final LongAdder controlSent = new LongAdder();
    private final LongAdder controlBuffered = new LongAdder();
    private final LongAdder controlBackpressured = new LongAdder();
    private final AtomicLong topologyRevision = new AtomicLong(1);
    private List<ClusterRoute> lastDirectoryRoutes = List.of();
    private List<byte[]> directoryChunks = List.of();
    private long directoryRevision;
    private volatile boolean running;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public RegistryServer(int port) {
        this.port = port;
        nodeConfig = RegistryNodeConfig.fromSystemProperties(port);
        clusterRouteTable = new RegistryClusterRouteTable(nodeConfig.nodeId(), nodeConfig.epoch());
        clusterService = new RegistryClusterService(nodeConfig, clusterRouteTable, this);
    }

    public synchronized void start() {
        if (running) return;
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors())));
        try {
            serverChannel = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(64 * 1024, 256 * 1024))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel channel) {
                            channel.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_BYTES, 0, 4, 0, 4))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new ReadTimeoutHandler(6))
                                    .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                        @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
                                            byte[] bytes = new byte[frame.readableBytes()];
                                            frame.readBytes(bytes);
                                            onFragment(new UnsafeBuffer(bytes), 0, bytes.length, ctx.channel());
                                        }
                                        @Override public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
                                            RegistrySession session = ctx.channel().attr(SESSION_KEY).get();
                                            if (session != null && ctx.channel().isWritable()) session.onWritable();
                                            super.channelWritabilityChanged(ctx);
                                        }
                                        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                            RegistrySession session = ctx.channel().attr(SESSION_KEY).getAndSet(null);
                                            if (session != null) cleanup(session);
                                            super.channelInactive(ctx);
                                        }
                                        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                            log.warn("Registry TCP connection failed remote={}: {}", ctx.channel().remoteAddress(), cause.toString());
                                            ctx.close();
                                        }
                                    });
                            channel.eventLoop().schedule(() -> {
                                if (channel.attr(SESSION_KEY).get() == null) channel.close();
                            }, 5, TimeUnit.SECONDS);
                        }
                    })
                    .bind(nodeConfig.clientBindHost(), port).syncUninterruptibly().channel();
            running = true;
            clusterService.start();
            maintenanceExecutor.scheduleWithFixedDelay(this::evictExpiredSessions, 1, 1, TimeUnit.SECONDS);
            maintenanceExecutor.scheduleWithFixedDelay(this::publishDirectorySafely, 0, 100, TimeUnit.MILLISECONDS);
            maintenanceExecutor.scheduleWithFixedDelay(this::publishTopologySafely, 0,
                    Math.max(1_000L, Long.getLong("kynet.registry.topologyRefreshMs", 5_000L)), TimeUnit.MILLISECONDS);
            log.info("Registry TCP started host={} port={}", nodeConfig.clientBindHost(), port);
        } catch (RuntimeException e) {
            stop(false);
            throw e;
        }
    }

    public synchronized void stop() { stop(true); }
    public synchronized void stopImmediately() { stop(false); }

    private void stop(boolean announceDeparture) {
        running = false;
        if (announceDeparture) clusterService.stop(); else clusterService.stopImmediately();
        maintenanceExecutor.shutdownNow();
        sessionsByInstanceId.values().forEach(RegistrySession::close);
        sessionsByInstanceId.clear();
        directoryDeliveries.clear();
        if (serverChannel != null) serverChannel.close().syncUninterruptibly();
        if (workerGroup != null) workerGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        if (bossGroup != null) bossGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    public RegistryClusterMetrics clusterMetrics() { return clusterService.metrics(); }
    public boolean isReady() { return running && clusterService.isReadyForClients(); }
    public RegistryMetrics registryMetrics() {
        return new RegistryMetrics(sessionsByInstanceId.size(), registrations.sum(), controlSent.sum(),
                controlBuffered.sum(), controlBackpressured.sum(), rejectedMessages.sum());
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, Channel channel) {
        if (!isReady()) {
            channel.close();
            return;
        }
        try {
            if (length < FrameCodec.FIXED_HEADER_LENGTH + Long.BYTES) { rejectedMessages.increment(); return; }
            int ordinal = FrameCodec.readMessageTypeOrdinal(buffer, offset);
            if (ordinal >= MESSAGE_TYPES.length) { rejectedMessages.increment(); return; }
            switch (MESSAGE_TYPES[ordinal]) {
                case REGISTER_SERVICE -> handleRegisterService(buffer, offset, length, channel);
                case REGISTER_ACTION -> handleRegisterAction(buffer, offset, length, channel);
                case SUBSCRIBE -> handleSubscribe(buffer, offset, length, channel);
                case HEARTBEAT -> handleHeartbeat(buffer, offset, channel);
                case UNREGISTER_SERVICE -> {
                    RegistrySession session = sessionsByInstanceId.get(FrameCodec.readSourceInstanceId(buffer, offset));
                    if (session != null && session.channel() == channel
                            && session.instanceMeta().instanceGeneration() == FrameCodec.readRequestId(buffer, offset)) cleanup(session);
                }
                default -> rejectedMessages.increment();
            }
        } catch (RuntimeException e) {
            rejectedMessages.increment();
            log.warn("Registry message rejected: {}", e.toString());
        }
    }

    private void handleRegisterService(DirectBuffer buffer, int offset, int length, Channel channel) {
        RegisterServiceRequest request = convertPayload(FrameCodec.decode(buffer, offset, length), RegisterServiceRequest.class);
        validateServiceRegistration(request);
        if (FrameCodec.readSourceInstanceId(buffer, offset) != request.getInstanceId()) {
            throw new IllegalArgumentException("Registration source instance does not match payload");
        }
        if (channel.attr(SESSION_KEY).get() != null) {
            rejectedMessages.increment();
            channel.close();
            return;
        }
        RegistrySession old = sessionsByInstanceId.get(request.getInstanceId());
        if (old != null) cleanup(old);
        InstanceMeta meta = new InstanceMeta(request.getServiceName(), request.getServiceId(), request.getInstanceId(),
                Math.max(1L, request.getInstanceGeneration()));
        RegistrySession session = new RegistrySession(channel, meta,
                Integer.getInteger("kynet.registry.sessionBufferBytes", 256 * 1024), controlSent);
        channel.attr(SESSION_KEY).set(session);
        sessionsByInstanceId.put(meta.instanceId(), session);
        clusterRouteTable.upEndpoint(meta, request.getDirectChannel(), request.getDirectStreamId(), request.getIncarnation());
        clusterService.onRoutesChanged();
        sendTopology(session);
        registrations.increment();
        log.info("Registered service name={} serviceId={} instanceId={}", meta.serviceName(), meta.serviceId(), meta.instanceId());
    }

    private void handleRegisterAction(DirectBuffer buffer, int offset, int length, Channel channel) {
        RegistrySession session = sessionsByInstanceId.get(FrameCodec.readSourceInstanceId(buffer, offset));
        if (session == null || session.channel() != channel) { rejectedMessages.increment(); return; }
        RegisterActionRequest request = convertPayload(FrameCodec.decode(buffer, offset, length), RegisterActionRequest.class);
        session.touch();
        clusterRouteTable.upAction(session.instanceMeta(), request.getAction());
        clusterService.onRoutesChanged();
    }

    private void handleSubscribe(DirectBuffer buffer, int offset, int length, Channel channel) {
        RegistrySession session = sessionsByInstanceId.get(FrameCodec.readSourceInstanceId(buffer, offset));
        if (session == null || session.channel() != channel) { rejectedMessages.increment(); return; }
        SubscribeRequest request = convertPayload(FrameCodec.decode(buffer, offset, length), SubscribeRequest.class);
        if (request.getTopic() == null || request.getTopic().isBlank()) throw new IllegalArgumentException("Empty topic");
        session.touch();
        clusterRouteTable.upTopic(session.instanceMeta(), request.getTopic());
        clusterService.onRoutesChanged();
    }

    private void handleHeartbeat(DirectBuffer buffer, int offset, Channel channel) {
        RegistrySession session = sessionsByInstanceId.get(FrameCodec.readSourceInstanceId(buffer, offset));
        if (session == null || session.channel() != channel
                || FrameCodec.readRequestId(buffer, offset) != session.instanceMeta().instanceGeneration()) return;
        session.touch();
        WireMessage reply = new WireMessage();
        reply.setMessageType(MessageType.HEARTBEAT);
        reply.setRequestId(session.instanceMeta().instanceGeneration());
        UnsafeBuffer encoded = BufferPool.acquireShared(128);
        try {
            int length = FrameCodec.encode(reply, encoded, 0);
            sendControl(session, encoded, 0, length);
        } finally { BufferPool.releaseShared(encoded); }
    }

    private RegistrySession.DeliveryResult sendControl(RegistrySession session, DirectBuffer buffer, int offset, int length) {
        RegistrySession.DeliveryResult result = session.send(buffer, offset, length);
        switch (result) {
            case SENT -> { }
            case BUFFERED -> controlBuffered.increment();
            case BACK_PRESSURED -> controlBackpressured.increment();
            case CLOSED -> { }
        }
        return result;
    }

    void onClusterMembershipChanged() {
        topologyRevision.incrementAndGet();
        broadcastTopology();
    }

    private void publishTopologySafely() {
        if (running) broadcastTopology();
    }

    private void broadcastTopology() {
        for (RegistrySession session : sessionsByInstanceId.values()) {
            try {
                sendTopology(session);
            } catch (RuntimeException e) {
                log.warn("Registry topology delivery failed instanceId={}: {}",
                        session.instanceMeta().instanceId(), e.toString());
            }
        }
    }

    private void sendTopology(RegistrySession session) {
        byte[] payload = RegistryTopologyCodec.encode(clusterService.topologyEndpoints());
        WireMessage message = new WireMessage();
        message.setMessageType(MessageType.REGISTRY_TOPOLOGY);
        message.setRequestId(topologyRevision.get());
        message.setPayloadType(RegistryTopologyCodec.class.getName());
        message.setPayload(payload);
        UnsafeBuffer buffer = BufferPool.acquireShared(FrameCodec.FIXED_HEADER_LENGTH + payload.length + 128);
        try {
            int length = FrameCodec.encode(message, buffer, 0);
            sendControl(session, buffer, 0, length);
        } finally {
            BufferPool.releaseShared(buffer);
        }
    }


    private <T> T convertPayload(WireMessage message, Class<T> type) {
        return ProtobufPayloadCodec.decode(message.getPayload(), type);
    }

    private void validateServiceRegistration(RegisterServiceRequest request) {
        if (request == null || request.getInstanceId() == 0 || request.getServiceName() == null
                || request.getServiceName().isBlank() || request.getServiceName().length() > 256
                || request.getDirectChannel() == null || request.getDirectChannel().isBlank()) {
            throw new IllegalArgumentException("Invalid service registration");
        }
        new ServiceDirectoryCodec.Endpoint(request.getInstanceId(), request.getIncarnation(), request.getServiceName(),
                request.getDirectChannel(), request.getDirectStreamId(), Set.of(), Set.of());
    }

    private void publishDirectorySafely() {
        if (!isReady()) return;
        try {
            directoryDeliveries.keySet().removeIf(session -> sessionsByInstanceId.get(session.instanceMeta().instanceId()) != session);
            List<ClusterRoute> routes = clusterRouteTable.snapshots();
            boolean changed = !routes.equals(lastDirectoryRoutes);
            if (changed || directoryChunks.isEmpty()) {
                Map<Integer, ClusterRoute> owners = new java.util.HashMap<>();
                for (ClusterRoute route : routes) {
                    if (route.kind() != RouteKind.SERVICE || route.incarnation() == 0) continue;
                    if (!route.ownerNodeId().equals(clusterService.nodeId()) && !clusterService.isRouteAlive(route)) continue;
                    owners.merge(route.instanceId(), route, (a, b) ->
                            a.instanceGeneration() > b.instanceGeneration()
                                    || a.instanceGeneration() == b.instanceGeneration() && a.ownerEpoch() > b.ownerEpoch() ? a : b);
                }
                Map<Integer, Set<Integer>> actionsByInstance = new java.util.HashMap<>();
                Map<Integer, Set<String>> topicsByInstance = new java.util.HashMap<>();
                for (ClusterRoute route : routes) {
                    ClusterRoute owner = owners.get(route.instanceId());
                    if (owner == null || route.instanceGeneration() != owner.instanceGeneration()
                            || !route.ownerNodeId().equals(owner.ownerNodeId()) || route.ownerEpoch() != owner.ownerEpoch()) continue;
                    if (route.kind() == RouteKind.ACTION) actionsByInstance.computeIfAbsent(route.instanceId(), id -> new HashSet<>()).add(route.action());
                    if (route.kind() == RouteKind.TOPIC && !route.topicName().isEmpty()) topicsByInstance.computeIfAbsent(route.instanceId(), id -> new HashSet<>()).add(route.topicName());
                }
                List<ServiceDirectoryCodec.Endpoint> endpoints = new java.util.ArrayList<>();
                for (ClusterRoute owner : owners.values()) {
                    Set<Integer> actions = actionsByInstance.getOrDefault(owner.instanceId(), Set.of());
                    Set<String> topics = topicsByInstance.getOrDefault(owner.instanceId(), Set.of());
                    endpoints.add(new ServiceDirectoryCodec.Endpoint(owner.instanceId(), owner.incarnation(),
                            owner.serviceName(), owner.endpointChannel(), owner.endpointStreamId(), actions, topics));
                }
                directoryChunks = ServiceDirectoryCodec.encode(++directoryRevision, endpoints);
                lastDirectoryRoutes = routes;
            }
            for (RegistrySession session : sessionsByInstanceId.values()) {
                DirectoryDelivery delivery = directoryDeliveries.computeIfAbsent(session,
                        key -> new DirectoryDelivery(directoryRevision, directoryChunks));
                if (delivery.index == delivery.chunks.size() && delivery.revision != directoryRevision) {
                    delivery = new DirectoryDelivery(directoryRevision, directoryChunks);
                    directoryDeliveries.put(session, delivery);
                }
                for (int batch = 0; batch < 4 && delivery.index < delivery.chunks.size(); batch++) {
                    byte[] payload = delivery.chunks.get(delivery.index);
                    WireMessage message = new WireMessage();
                    message.setMessageType(MessageType.SERVICE_DIRECTORY);
                    message.setRequestId(session.instanceMeta().instanceGeneration());
                    message.setPayload(payload);
                    UnsafeBuffer buffer = BufferPool.acquireShared(payload.length + 128);
                    try {
                        int length = FrameCodec.encode(message, buffer, 0);
                        RegistrySession.DeliveryResult result = sendControl(session, buffer, 0, length);
                        if (result == RegistrySession.DeliveryResult.SENT || result == RegistrySession.DeliveryResult.BUFFERED) delivery.index++;
                        else break;
                    } finally { BufferPool.releaseShared(buffer); }
                }
            }
        } catch (RuntimeException e) {
            log.warn("Directory publication failed: {}", e.toString());
        }
    }


    private void evictExpiredSessions() {
        long now = System.nanoTime();
        for (RegistrySession session : sessionsByInstanceId.values()) {
            if (session.isExpired(now, SESSION_TIMEOUT_NANOS)) cleanup(session);
        }
    }

    private void cleanup(RegistrySession session) {
        directoryDeliveries.remove(session);
        if (sessionsByInstanceId.remove(session.instanceMeta().instanceId(), session)) {
            clusterRouteTable.downInstance(session.instanceMeta().instanceId());
            clusterService.onRoutesChanged();
        }
        session.close();
    }

    private static final class DirectoryDelivery {
        final long revision;
        final List<byte[]> chunks;
        int index;
        DirectoryDelivery(long revision, List<byte[]> chunks) { this.revision = revision; this.chunks = chunks; }
    }
}
