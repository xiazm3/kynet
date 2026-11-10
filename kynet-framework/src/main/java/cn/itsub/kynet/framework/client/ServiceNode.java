package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.codec.EventWireCodec;
import cn.itsub.kynet.common.codec.BroadcastPayloadCodec;
import cn.itsub.kynet.common.codec.GatewayKickWireCodec;
import cn.itsub.kynet.common.codec.GatewayPushWireCodec;
import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.codec.RpcWireCodec;
import cn.itsub.kynet.common.protocol.RegistryTopologyCodec;
import cn.itsub.kynet.common.protocol.ServiceDirectoryCodec;
import cn.itsub.kynet.common.protocol.ServiceDirectoryCodec.Endpoint;
import cn.itsub.kynet.common.protocol.GatewayPushMessage;
import cn.itsub.kynet.common.protocol.MessageType;
import cn.itsub.kynet.common.protocol.PlayerBroadcastEnvelope;
import cn.itsub.kynet.common.protocol.RegisterActionRequest;
import cn.itsub.kynet.common.protocol.RegisterServiceRequest;
import cn.itsub.kynet.common.protocol.SubscribeRequest;
import cn.itsub.kynet.common.protocol.WireMessage;
import cn.itsub.kynet.common.transport.BufferPool;
import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.common.util.ServiceIds;
import cn.itsub.kynet.framework.action.ActionInvocation;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.gateway.GatewayKickHandler;
import cn.itsub.kynet.framework.gateway.GatewayPushHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ServiceNode {
    private static final Logger log = LoggerFactory.getLogger(ServiceNode.class);
    private static final long HEARTBEAT_INTERVAL_MILLIS = Math.max(50L,
            Long.getLong("kynet.registry.heartbeatIntervalMs",
                    Long.getLong("kynet.broker.heartbeatIntervalMs", 250L)));
    private static final long RECONNECT_DELAY_MILLIS = Math.max(10L,
            Long.getLong("kynet.registry.reconnectDelayMs",
                    Long.getLong("kynet.broker.reconnectDelayMs", 100L)));
    private static final long MAX_RECONNECT_DELAY_MILLIS = Math.max(RECONNECT_DELAY_MILLIS,
            Long.getLong("kynet.registry.maxReconnectDelayMs", 5_000L));
    private static final long RPC_TIMEOUT_SCAN_INTERVAL_MILLIS = Math.max(1L,
            Long.getLong("kynet.rpc.timeoutScanMillis", 10L));

    private final AtomicInteger requestIdStripeSeq = new AtomicInteger();
    private final ThreadLocal<long[]> requestIdStripe = ThreadLocal.withInitial(() ->
            new long[]{(long) requestIdStripeSeq.getAndIncrement() << 32});
    private final AtomicLong endpointCursor = new AtomicLong();
    private final AtomicLong registrationGeneration = new AtomicLong();
    private final AtomicLong connectionGeneration = new AtomicLong();
    private final AtomicBoolean requestMaintenanceStarted = new AtomicBoolean();
    private final Object connectionLock = new Object();
    private final Object topologyLock = new Object();
    private volatile List<RegistryEndpoint> registryEndpoints = List.of();
    private final Set<RegistryEndpoint> seedRegistryEndpoints = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService reconnectExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService requestMaintenanceExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ServiceInvokeDispatcher invokeDispatcher;
    private final ServiceDirectory directory = new ServiceDirectory();
    private final PeerTransport peerTransport;
    private final RegistryConnection registryConnection;
    private final LongPendingRequestMap pendingRequests = new LongPendingRequestMap();
    private final Map<Integer, PayloadDecoder<?>> responseDecoders = new ConcurrentHashMap<>();
    private final Map<String, TopicSubscriber> topicSubscribers = new ConcurrentHashMap<>();
    private final Map<Integer, ActionInvocation> desiredActionHandlers = new ConcurrentHashMap<>();
    private final Set<Integer> desiredFastActions = ConcurrentHashMap.newKeySet();
    private volatile GatewayPushHandler gatewayPushHandler;
    private volatile GatewayKickHandler gatewayKickHandler;
    private final String serviceName;
    private final int serviceId;
    private final int instanceId;
    private ScheduledExecutorService heartbeatExecutor;
    private volatile RegistryEndpoint activeEndpoint;
    private volatile boolean started;
    private volatile boolean closed;
    private volatile boolean reconnectScheduled;
    private int reconnectAttempt;
    private long lastTopologyRevision = -1;
    private volatile long lastRegistryContact;

    public ServiceNode(String addr) {
        this(parseHost(addr), parsePort(addr), "AnonymousService");
    }

    public ServiceNode(String host, int port, String serviceName) {
        this.serviceName = serviceName;
        this.serviceId = ServiceIds.serviceId(serviceName);
        this.instanceId = ServiceIds.newInstanceId();
        this.peerTransport = new PeerTransport(instanceId, serviceName, directory, this::onDirectFragment);
        this.registryConnection = new RegistryConnection(this::onRegistryFrame, this::onRegistryDisconnect);
        addSeedRegistryEndpoint(host, port);
        this.invokeDispatcher = new ServiceInvokeDispatcher(this, new ServiceInvokeDispatcher.RpcResponseWriter() {
            @Override
            public void sendSuccess(long requestId, String traceId, int action, int targetInstanceId,
                                    byte[] body, boolean attachmentUpdated,
                                    byte[] attachmentData) {
                sendRpcSuccessResponse(requestId, traceId, action, targetInstanceId, body,
                        attachmentUpdated, attachmentData, 0L);
            }

            @Override
            public void sendError(long requestId, String traceId, int action, int targetInstanceId,
                                  String errorCode, String errorMessage,
                                  boolean attachmentUpdated,
                                  byte[] attachmentData) {
                sendRpcErrorResponse(requestId, traceId, action, targetInstanceId, errorCode, errorMessage,
                        attachmentUpdated, attachmentData, 0L);
            }
            @Override
            public void sendSuccess(long requestId, String traceId, int action, int targetInstanceId, byte[] body,
                                    boolean attachmentUpdated, byte[] attachmentData, long incarnation) {
                sendRpcSuccessResponse(requestId, traceId, action, targetInstanceId, body, attachmentUpdated, attachmentData, incarnation);
            }
            @Override
            public void sendError(long requestId, String traceId, int action, int targetInstanceId, String errorCode, String errorMessage,
                                  boolean attachmentUpdated, byte[] attachmentData, long incarnation) {
                sendRpcErrorResponse(requestId, traceId, action, targetInstanceId, errorCode, errorMessage, attachmentUpdated, attachmentData, incarnation);
            }
        });
    }

    public ServiceNode(List<String> addresses, String serviceName) {
        this(firstAddress(addresses).host(), firstAddress(addresses).port(), serviceName);
        for (String address : addresses) {
            addSeedRegistryEndpoint(parseHost(address), parsePort(address));
        }
    }

    public void start() {
        if (closed) throw new IllegalStateException("Service node is closed");
        if (registryEndpoints.isEmpty()) throw new IllegalArgumentException("At least one registry seed is required");
        peerTransport.start();
        startRequestMaintenance();
        try {
            connectOrThrow();
        } catch (IllegalStateException e) {
            log.warn("Initial registry connection failed; retrying: {}", e.getMessage());
            scheduleReconnect();
        }
    }

    private void connectOrThrow() {
        synchronized (connectionLock) {
            if (started) {
                return;
            }
            List<RegistryEndpoint> endpoints = registryEndpoints;
            if (endpoints.isEmpty()) {
                throw new IllegalStateException("No registry endpoints are available.");
            }
            Exception lastFailure = null;
            int startIndex = Math.floorMod((int) endpointCursor.getAndIncrement(), endpoints.size());
            for (int i = 0; i < endpoints.size(); i++) {
                RegistryEndpoint endpoint = endpoints.get((startIndex + i) % endpoints.size());
                try {
                    long generation = connectionGeneration.incrementAndGet();
                    registryConnection.connect(endpoint.host(), endpoint.port(), generation);
                    activeEndpoint = endpoint;
                    synchronized (topologyLock) { lastTopologyRevision = -1; }
                    lastRegistryContact = System.nanoTime();
                    started = true;
                    reconnectAttempt = 0;
                    heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
                    replayRegistrations();
                    heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeatSafely,
                            HEARTBEAT_INTERVAL_MILLIS,
                            HEARTBEAT_INTERVAL_MILLIS,
                            TimeUnit.MILLISECONDS);
                    startRequestMaintenance();
                    log.info("Service node connected to registry endpoint={}:{}", endpoint.host(), endpoint.port());
                    return;
                } catch (Exception e) {
                    lastFailure = e;
                    started = false;
                    activeEndpoint = null;
                    shutdownExecutor(heartbeatExecutor);
                    closeConnectionResources();
                }
            }
            throw new IllegalStateException("Failed to connect any registry endpoint " + endpoints, lastFailure);
        }
    }

    private void onRegistryDisconnect(long generation, String reason) {
        if (!closed && connectionGeneration.get() == generation) handleDisconnect(reason);
    }

    public void shutdown() {
        if (started) {
            try {
                WireMessage unregister = new WireMessage();
                unregister.setMessageType(MessageType.UNREGISTER_SERVICE);
                unregister.setSourceInstanceId(instanceId);
                unregister.setRequestId(registrationGeneration.get());
                send(unregister);
            } catch (RuntimeException ignored) { }
        }
        closed = true;
        synchronized (connectionLock) {
            started = false;
            connectionGeneration.incrementAndGet();
            shutdownExecutor(heartbeatExecutor);
            closeConnectionResources();
        }
        reconnectExecutor.shutdownNow();
        requestMaintenanceExecutor.shutdownNow();
        registryConnection.close();
        peerTransport.close();
        failAllPendingRequests("REGISTRY_CLOSED", "Service node is closed.");
        invokeDispatcher.shutdown();
    }

    public void publish(String topic, byte[] payload) {
        int estimatedSize = 2 + (topic == null ? 0 : topic.length()) + 4 + (payload == null ? 0 : payload.length);
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(Math.max(64, estimatedSize));
        int payloadLength = EventWireCodec.encode(payloadBuffer, 0, topic, payload);
        try {
            sendFrame(MessageType.EVENT_PUBLISH, 0L, 0, 0, topic.hashCode(), topic, null, null,
                    payloadBuffer, 0, payloadLength);
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    public void subscribe(String topic, TopicSubscriber subscriber) {
        topicSubscribers.put(topic, subscriber);
        if (started) {
            sendSubscribe(topic);
        }
    }

    private void sendSubscribe(String topic) {
        SubscribeRequest request = new SubscribeRequest();
        request.setTopic(topic);
        request.setServiceId(serviceId);
        request.setInstanceId(instanceId);

        WireMessage message = new WireMessage();
        message.setMessageType(MessageType.SUBSCRIBE);
        message.setTopic(topic);
        message.setSourceServiceId(serviceId);
        message.setSourceInstanceId(instanceId);
        message.setPayloadType(SubscribeRequest.class.getName());
        message.setPayload(ProtobufPayloadCodec.encode(request));
        send(message);
    }

    public ActionResult invoke(int cmd, int act, Object payload) {
        return invoke(cmd, act, payload, InvokeOptions.defaults());
    }

    public CompletableFuture<ActionResult> invokeAsync(int cmd, int act, Object payload) {
        return invokeAsync(cmd, act, payload, InvokeOptions.defaults());
    }

    public void invokeOneWay(int cmd, int act, Object payload) {
        invokeOneWay(cmd, act, payload, InvokeOptions.defaults());
    }

    public ActionResult invoke(int cmd, int act, Object payload, InvokeOptions options) {
        InvokeOptions resolvedOptions = resolveOptions(options);
        try {
            return invokeAsync(cmd, act, payload, resolvedOptions)
                    .get(resolvedOptions.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("RPC interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("RPC execution failed", e);
        } catch (TimeoutException e) {
            return ActionResult.error(
                    "RESULT_UNKNOWN",
                    "RPC result is unknown after timeout of " + resolvedOptions.getTimeout().toMillis() + " ms",
                    resolvedOptions.getAttachmentData(),
                    false);
        }
    }

    public void invokeOneWay(int cmd, int act, Object payload, InvokeOptions options) {
        InvokeOptions resolvedOptions = resolveOptions(options);
        sendRpcRequest(nextRequestId(), ActionKey.of(cmd, act), payload, resolvedOptions, true);
    }

    public FastOneWaySender prepareFastOneWaySender(int cmd, int act, Object payload, InvokeOptions options) {
        return new FastOneWaySender(ActionKey.of(cmd, act), payload, resolveOptions(options));
    }

    public FastRpcSender prepareFastRpcSender(int cmd, int act, Object payload, InvokeOptions options) {
        return new FastRpcSender(ActionKey.of(cmd, act), payload, resolveOptions(options));
    }

    public CompletableFuture<ActionResult> invokeAsync(int cmd, int act, Object payload, InvokeOptions options) {
        InvokeOptions resolvedOptions = resolveOptions(options);
        long requestId = nextRequestId();
        int action = ActionKey.of(cmd, act);
        CompletableFuture<ActionResult> future = new CompletableFuture<>();
        byte[] requestPayload = resolvedOptions.isIdempotent()
                ? encodeRpcRequest(payload, resolvedOptions, false)
                : null;
        PendingRequest pending = PendingRequest.future(action, future,
                responseDecoders.get(action), resolvedOptions, requestPayload);
        pendingRequests.put(requestId, pending);
        if (resolvedOptions.isIdempotent()) {
            sendPendingRequest(requestId, pending, false);
        } else {
            sendDirectPendingRequest(requestId, pending, payload, resolvedOptions);
        }
        return future;
    }

    public <T> void registerResponseDecoder(int cmd, int act, Class<T> targetType) {
        registerResponseDecoder(ActionKey.of(cmd, act),
                (buffer, offset, length) -> ProtobufPayloadCodec.decode(buffer, offset, length, targetType));
    }

    public void registerResponseDecoder(int action, PayloadDecoder<?> decoder) {
        if (decoder == null) {
            responseDecoders.remove(action);
            return;
        }
        responseDecoders.put(action, decoder);
    }

    public void registerActionHandler(int action, ActionInvocation invocation) {
        desiredActionHandlers.put(action, invocation);
        invokeDispatcher.registerActionHandler(action, invocation);
        if (started) {
            sendActionRegistration(action);
        }
    }

    private void sendActionRegistration(int action) {
        RegisterActionRequest request = new RegisterActionRequest();
        request.setAction(action);
        request.setServiceId(serviceId);
        request.setInstanceId(instanceId);

        WireMessage message = new WireMessage();
        message.setMessageType(MessageType.REGISTER_ACTION);
        message.setAction(action);
        message.setSourceServiceId(serviceId);
        message.setSourceInstanceId(instanceId);
        message.setPayloadType(RegisterActionRequest.class.getName());
        message.setPayload(ProtobufPayloadCodec.encode(request));
        send(message);
    }

    public void pushToGateway(long sessionId, long userId, int cmd, int act, Object payload) {
        pushToGateway(sessionId, userId, cmd, act, payload, 0);
    }

    public void pushToGateway(long sessionId, long userId, int cmd, int act, Object payload, int targetGatewayInstanceId) {
        byte[] encodedPayload = BroadcastPayloadCodec.encode(payload);
        int estimatedSize = 8 + 8 + 4 + 4
                + 4 + (encodedPayload == null ? 0 : encodedPayload.length);
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(Math.max(64, estimatedSize));
        int payloadLength = GatewayPushWireCodec.encode(payloadBuffer, 0, sessionId, userId, cmd, act, encodedPayload);
        try {
            sendFrame(MessageType.GATEWAY_PUSH, 0L, act, targetGatewayInstanceId, 0, null, null, null,
                    payloadBuffer, 0, payloadLength);
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    public void broadcast(int msgId, Object payload, Collection<Long> playerIds) {
        PlayerBroadcastEnvelope envelope = new PlayerBroadcastEnvelope();
        envelope.setMsgId(msgId);
        if (playerIds != null && !playerIds.isEmpty()) {
            envelope.setPlayerIds(playerIds.stream()
                    .filter(id -> id != null && id > 0)
                    .toList());
        }
        envelope.setPayload(BroadcastPayloadCodec.encode(payload));
        int cmd = ActionKey.cmdOf(msgId);
        int act = ActionKey.actOf(msgId);
        pushToGateway(0L, 0L, cmd, act, envelope, 0);
    }

    /**
     * 服务端权威踢人：关闭目标网关连接，{@code UserHook.quit} 照常触发。
     * {@code sessionId > 0} 精确踢该连接；否则 {@code userId > 0} 踢该用户当前所有连接，
     * 但跳过 {@code exceptSessionId}（顶号场景保留新会话）。
     * {@code targetGatewayInstanceId} 为 0 时扇出给全部网关，由各网关本地解析；
     * 已知承载网关时传入其实例号可精准投递。网关侧记入 kickedConnectionCount。
     */
    public void kick(long sessionId, long userId, long exceptSessionId,
                     int targetGatewayInstanceId, String reason) {
        if (sessionId <= 0 && userId <= 0) {
            throw new IllegalArgumentException("kick requires a positive sessionId or userId");
        }
        if (exceptSessionId < 0) {
            throw new IllegalArgumentException("exceptSessionId must not be negative");
        }
        byte[] reasonBytes = reason == null ? null : reason.getBytes(StandardCharsets.UTF_8);
        int estimatedSize = 24 + 2 + (reasonBytes == null ? 0 : reasonBytes.length);
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(Math.max(64, estimatedSize));
        try {
            int payloadLength = GatewayKickWireCodec.encode(payloadBuffer, 0,
                    sessionId, userId, exceptSessionId, reason);
            sendFrame(MessageType.GATEWAY_KICK, 0L, 0, targetGatewayInstanceId, 0,
                    null, null, null, payloadBuffer, 0, payloadLength);
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    public void kickSession(long sessionId, String reason) {
        kick(sessionId, 0L, 0L, 0, reason);
    }

    public void kickUser(long userId, String reason) {
        kick(0L, userId, 0L, 0, reason);
    }

    public void kickUserExcept(long userId, long exceptSessionId, String reason) {
        kick(0L, userId, exceptSessionId, 0, reason);
    }

    public int getServiceId() { return serviceId; }
    public int getInstanceId() { return instanceId; }
    public String getServiceName() { return serviceName; }

    public void setGatewayPushHandler(GatewayPushHandler gatewayPushHandler) {
        this.gatewayPushHandler = gatewayPushHandler;
    }

    public void setGatewayKickHandler(GatewayKickHandler gatewayKickHandler) {
        this.gatewayKickHandler = gatewayKickHandler;
    }

    public void setFastOneWayHandler(FastOneWayHandler fastOneWayHandler) {
        invokeDispatcher.setFastOneWayHandler(fastOneWayHandler);
    }

    public void registerFastOneWayHandler(int cmd, int act, FastOneWayHandler fastOneWayHandler) {
        registerFastOneWayHandler(ActionKey.of(cmd, act), fastOneWayHandler);
    }

    public void registerFastOneWayHandler(int action, FastOneWayHandler fastOneWayHandler) {
        invokeDispatcher.registerFastOneWayHandler(action, fastOneWayHandler);
        desiredFastActions.add(action);
        if (started) sendActionRegistration(action);
    }

    private void registerService() {
        RegisterServiceRequest request = new RegisterServiceRequest();
        request.setServiceName(serviceName);
        request.setServiceId(serviceId);
        request.setInstanceId(instanceId);
        request.setInstanceGeneration(registrationGeneration.incrementAndGet());
        directory.reconnect(request.getInstanceGeneration());
        Endpoint direct = peerTransport.local();
        request.setDirectChannel(direct.channel());
        request.setDirectStreamId(direct.streamId());
        request.setIncarnation(direct.incarnation());

        WireMessage message = new WireMessage();
        message.setMessageType(MessageType.REGISTER_SERVICE);
        message.setSourceServiceId(serviceId);
        message.setSourceInstanceId(instanceId);
        message.setPayloadType(RegisterServiceRequest.class.getName());
        message.setPayload(ProtobufPayloadCodec.encode(request));
        send(message);
    }

    private void replayRegistrations() {
        registerService();
        for (Integer action : desiredActionHandlers.keySet()) {
            sendActionRegistration(action);
        }
        for (Integer action : desiredFastActions) {
            if (!desiredActionHandlers.containsKey(action)) sendActionRegistration(action);
        }
        for (String topic : topicSubscribers.keySet()) {
            sendSubscribe(topic);
        }
    }

    private void onRegistryFrame(byte[] bytes, long generation) {
        if (!started || connectionGeneration.get() != generation) return;
        onFragment(new UnsafeBuffer(bytes), 0, bytes.length, generation);
    }

    private void onFragment(DirectBuffer buffer, int offset, int length, long generation) {
        FrameCodec.DecodedFrame frame = FrameCodec.decodeView(buffer, offset, length);
        if (frame.messageType() == null) {
            log.debug("Ignored unknown frame type at offset {}", offset);
            return;
        }
        if (frame.messageType() == MessageType.SERVICE_DIRECTORY) {
            if (frame.requestId() != registrationGeneration.get()) return;
            lastRegistryContact = System.nanoTime();
            byte[] payload = new byte[frame.payloadLength()];
            frame.payloadBuffer().getBytes(frame.payloadOffset(), payload);
            if (directory.accept(ServiceDirectoryCodec.decode(payload), frame.requestId()) && directory.instance(instanceId) == null) {
                handleDisconnect("Registry lost this instance registration");
            }
        } else if (frame.messageType() == MessageType.HEARTBEAT && frame.requestId() == registrationGeneration.get()) {
            lastRegistryContact = System.nanoTime();
        } else if (frame.messageType() == MessageType.REGISTRY_TOPOLOGY) {
            handleRegistryTopology(frame, generation);
        }
    }

    private void onDirectFragment(DirectBuffer buffer, int offset, int length, long sourceIncarnation) {
        if ((buffer.getByte(offset + FrameCodec.OFFSET_MESSAGE_TYPE) & 0xFF) == MessageType.RPC_REQUEST.ordinal()
                && invokeDispatcher.tryHandleFastOneWayFrame(buffer, offset, length)) {
            return;
        }
        FrameCodec.DecodedFrame frame = FrameCodec.decodeView(buffer, offset, length);
        if (frame.messageType() == MessageType.RPC_REQUEST) invokeDispatcher.handleRpcRequest(frame, sourceIncarnation);
        else if (frame.messageType() == MessageType.RPC_RESPONSE) {
            PendingRequest pending = pendingRequests.get(frame.requestId());
            if (pending != null && pending.selectedTarget != null
                    && pending.selectedTarget.incarnation() != sourceIncarnation) return;
            handleRpcResponse(frame);
        } else if (isBusinessMessage(frame.messageType())) handleIncoming(frame);
    }

    private static boolean isBusinessMessage(MessageType type) {
        return type == MessageType.RPC_REQUEST || type == MessageType.RPC_RESPONSE
                || type == MessageType.GATEWAY_PUSH || type == MessageType.GATEWAY_KICK
                || type == MessageType.EVENT_PUBLISH
                || type == MessageType.EVENT_DELIVER;
    }

    private void ensureDataStarted() {
        if (closed) throw new IllegalStateException("Service node is closed");
        if (!peerTransport.running()) start();
    }

    public int discoveredInstanceCount() { return directory.endpoints().size(); }
    public int knownRegistryEndpointCount() { return registryEndpoints.size(); }
    public boolean isRegistryConnected() { return started; }
    public long directSentCount() { return peerTransport.sentCount(); }
    public long directIpcSentCount() { return peerTransport.ipcSentCount(); }
    public long directUdpSentCount() { return peerTransport.udpSentCount(); }
    public long directDroppedCount() { return peerTransport.droppedCount(); }
    public long directOfferRetryCount() { return peerTransport.offerRetryCount(); }
    public long directQueuedBytes() { return peerTransport.queuedBytes(); }

    private void sendBusinessFrame(DirectBuffer buffer, int offset, int length, long routingKey) {
        ensureDataStarted();
        FrameCodec.DecodedFrame frame = FrameCodec.decodeView(buffer, offset, length);
        if (frame.messageType() == MessageType.RPC_REQUEST) {
            PendingRequest pending = pendingRequests.get(frame.requestId());
            Endpoint endpoint = pending == null ? null : pending.selectedTarget;
            if (endpoint == null) {
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                        Math.max(0L, Long.getLong("kynet.direct.discoveryWaitMs", 1000L)));
                if (pending != null) deadline = Math.min(deadline, pending.deadlineNanos);
                do {
                    endpoint = directory.select(frame.action(), frame.targetInstanceId(), routingKey);
                    if (endpoint != null || !started || System.nanoTime() >= deadline) break;
                    java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                } while (!closed);
                if (endpoint == null) {
                    log.warn("NO_PROVIDER action={} targetInstance={} discoveredInstances={} started={}",
                            frame.action(), frame.targetInstanceId(), directory.endpoints().size(), started);
                    throw new IllegalStateException("NO_PROVIDER: no direct provider for action " + frame.action());
                }
                log.debug("RPC route action={} -> instanceId={} incarnation={} channel={}",
                        frame.action(), endpoint.instanceId(), endpoint.incarnation(), endpoint.channel());
                if (pending != null) pending.selectedTarget = endpoint;
            }
            peerTransport.send(endpoint, buffer, offset, length);
        } else if (frame.messageType() == MessageType.RPC_RESPONSE) {
            Endpoint endpoint = peerTransport.replyEndpoint(frame.targetInstanceId());
            if (endpoint == null) {
                log.warn("No reply endpoint for instanceId={} requestId={} discovered={}",
                        frame.targetInstanceId(), frame.requestId(), directory.endpoints().size());
                throw new IllegalStateException("No reply endpoint for " + frame.targetInstanceId());
            }
            peerTransport.send(endpoint, buffer, offset, length);
        } else {
            boolean gatewayDirected = frame.messageType() == MessageType.GATEWAY_PUSH
                    || frame.messageType() == MessageType.GATEWAY_KICK;
            String topic = gatewayDirected ? null
                    : EventWireCodec.decodeView(frame.payloadBuffer(), frame.payloadOffset(), frame.payloadLength()).topic();
            RuntimeException failure = null;
            int matches = 0;
            for (Endpoint endpoint : topic == null ? directory.service("GatewayService") : directory.topic(topic)) {
                boolean match = gatewayDirected
                        ? "GatewayService".equals(endpoint.serviceName())
                            && (frame.targetInstanceId() == 0 || frame.targetInstanceId() == endpoint.instanceId())
                        : endpoint.topics().contains(topic);
                if (!match) continue;
                matches++;
                try { peerTransport.send(endpoint, buffer, offset, length); }
                catch (RuntimeException e) { failure = e; }
            }
            if (failure != null) throw new IllegalStateException("Direct fan-out partially rejected", failure);
            if (matches == 0 && frame.targetInstanceId() != 0) throw new IllegalStateException("No direct push target");
        }
    }

    private void handleIncoming(FrameCodec.DecodedFrame frame) {
        switch (frame.messageType()) {
            case EVENT_DELIVER, EVENT_PUBLISH -> handleEventDeliver(frame);
            case RPC_REQUEST -> invokeDispatcher.handleRpcRequest(frame);
            case RPC_RESPONSE -> handleRpcResponse(frame);
            case GATEWAY_PUSH -> handleGatewayPush(frame);
            case GATEWAY_KICK -> handleGatewayKick(frame);
            case REGISTRY_TOPOLOGY -> handleRegistryTopology(frame, connectionGeneration.get());
            default -> log.debug("Ignored message type {}", frame.messageType());
        }
    }

    private void handleRegistryTopology(FrameCodec.DecodedFrame frame, long generation) {
        byte[] payload = new byte[frame.payloadLength()];
        frame.payloadBuffer().getBytes(frame.payloadOffset(), payload);
        List<RegistryTopologyCodec.Endpoint> topology = RegistryTopologyCodec.decode(payload);
        synchronized (topologyLock) {
            if (!started || connectionGeneration.get() != generation
                    || frame.requestId() <= lastTopologyRevision) return;
            LinkedHashSet<RegistryEndpoint> next = new LinkedHashSet<>();
            for (RegistryEndpoint endpoint : registryEndpoints) {
                if (seedRegistryEndpoints.contains(endpoint)) next.add(endpoint);
            }
            for (RegistryTopologyCodec.Endpoint endpoint : topology) {
                if ("ALIVE".equals(endpoint.state()) && endpoint.host() != null
                        && !endpoint.host().isBlank() && endpoint.port() > 0 && endpoint.port() <= 65_535) {
                    next.add(new RegistryEndpoint(endpoint.host(), endpoint.port()));
                }
            }
            registryEndpoints = List.copyOf(next);
            lastTopologyRevision = frame.requestId();
        }
    }

    private void handleEventDeliver(FrameCodec.DecodedFrame frame) {
        EventWireCodec.EventView event = EventWireCodec.decodeView(
                frame.payloadBuffer(),
                frame.payloadOffset(),
                frame.payloadLength());
        String topic = event.topic();
        TopicSubscriber subscriber = topicSubscribers.get(topic);
        if (subscriber != null) {
            subscriber.onMessage(topic, event.copyPayload());
        }
    }

    private void handleRpcResponse(FrameCodec.DecodedFrame frame) {
        RpcWireCodec.RpcResponseView response = RpcWireCodec.decodeResponseView(
                frame.payloadBuffer(),
                frame.payloadOffset(),
                frame.payloadLength());
        PendingRequest pending = pendingRequests.get(frame.requestId());
        if (pending == null) {
            return;
        }
        if (pending.selectedTarget != null && pending.selectedTarget.instanceId() != frame.sourceInstanceId()) {
            log.warn("RPC response instance mismatch requestId={} expected={} actual={}",
                    frame.requestId(), pending.selectedTarget.instanceId(), frame.sourceInstanceId());
            return;
        }
        if (!response.success()
                && isRetryableRpcError(response.errorCode())
                && pending.scheduleRetry(System.nanoTime())) {
            return;
        }
        if (!pendingRequests.remove(frame.requestId(), pending)) {
            return;
        }
        if (pending.fastCallback() != null) {
            try {
                pending.fastCallback().onResponse(response.success(), response);
            } catch (RuntimeException e) {
                log.warn("Fast RPC callback failed for action {}: {}", pending.action(), e.getMessage());
            }
            return;
        }
        byte[] attachmentData = response.attachmentUpdated() ? response.copyAttachmentData() : null;
        if (response.success()) {
            pending.future().complete(ActionResult.success(
                    decodeResponseBody(pending, response),
                    attachmentData,
                    response.attachmentUpdated()));
        } else {
            pending.future().complete(ActionResult.error(
                    response.errorCode(),
                    response.errorMessage(),
                    attachmentData,
                    response.attachmentUpdated()));
        }
    }

    private void handleGatewayPush(FrameCodec.DecodedFrame frame) {
        GatewayPushHandler handler = this.gatewayPushHandler;
        if (handler != null) {
            GatewayPushWireCodec.GatewayPushView pushView = GatewayPushWireCodec.decodeView(
                    frame.payloadBuffer(),
                    frame.payloadOffset(),
                    frame.payloadLength());
            GatewayPushMessage push = new GatewayPushMessage();
            push.setSessionId(pushView.sessionId());
            push.setUserId(pushView.userId());
            push.setCmd(pushView.cmd());
            push.setAct(pushView.act());
            push.setPayload(pushView.copyPayload());
            handler.onPush(push);
        } else {
            log.info("Gateway push ignored because no handler is registered");
        }
    }

    private void handleGatewayKick(FrameCodec.DecodedFrame frame) {
        GatewayKickHandler handler = this.gatewayKickHandler;
        if (handler == null) {
            log.info("Gateway kick ignored because no handler is registered");
            return;
        }
        GatewayKickWireCodec.GatewayKickView kickView = GatewayKickWireCodec.decodeView(
                frame.payloadBuffer(),
                frame.payloadOffset(),
                frame.payloadLength());
        handler.onKick(kickView.sessionId(), kickView.userId(), kickView.exceptSessionId(), kickView.reason());
    }

    private void send(WireMessage message) {
        if (!started) {
            ensureConnected();
        }
        try {
            doSend(message);
        } catch (RegistryConnection.SendException e) {
            if (e.fatal() && started) handleDisconnect(e.getMessage());
            throw e;
        }
    }

    private void doSend(WireMessage message) {
        UnsafeBuffer buf = null;
        try {
            buf = BufferPool.acquireShared(estimateFrameSize(
                    message.getTopic(),
                    message.getTraceId(),
                    message.getPayloadType(),
                    message.getPayload() == null ? 0 : message.getPayload().length));
            int len = FrameCodec.encode(message, buf, 0);
            registryConnection.send(buf, 0, len);
        } finally {
            BufferPool.releaseShared(buf);
        }
    }

    private void sendFrame(MessageType messageType, long requestId, int action, int targetInstanceId, int topicHash,
                           String topic, String traceId, String payloadType,
                           DirectBuffer payloadBuffer, int payloadOffset, int payloadLength) {
        boolean business = isBusinessMessage(messageType);
        if (business) ensureDataStarted();
        else if (!started) {
            ensureConnected();
        }
        UnsafeBuffer frameBuffer = null;
        try {
            long routingKey = requestId;
            if (messageType == MessageType.RPC_REQUEST) {
                RpcWireCodec.RpcRequestView request = RpcWireCodec.decodeRequestView(
                        payloadBuffer, payloadOffset, payloadLength);
                routingKey = rpcRoutingKey(request.sessionId(), request.userId(), requestId);
            }
            frameBuffer = BufferPool.acquireShared(estimateFrameSize(topic, traceId, payloadType, payloadLength));
            int frameLength = FrameCodec.encode(
                    messageType,
                    action,
                    instanceId,
                    targetInstanceId,
                    topicHash,
                    requestId,
                    topic,
                    traceId,
                    payloadType,
                    payloadBuffer,
                    payloadOffset,
                    payloadLength,
                    frameBuffer,
                    0);
            if (business) sendBusinessFrame(frameBuffer, 0, frameLength, routingKey);
            else registryConnection.send(frameBuffer, 0, frameLength);
        } catch (RegistryConnection.SendException e) {
            if (!business && e.fatal() && started) {
                handleDisconnect(e.getMessage());
            }
            throw e;
        } catch (Exception e) {
            if (!business && started) {
                handleDisconnect("Registry send failed: " + e.getMessage());
            }
            throw new IllegalStateException("Failed to send registry message.", e);
        } finally {
            BufferPool.releaseShared(frameBuffer);
        }
    }

    private int estimateFrameSize(String topic, String traceId, String payloadType, int payloadLength) {
        return FrameCodec.FIXED_HEADER_LENGTH
                + Long.BYTES
                + Short.BYTES * 3
                + utf8Length(topic)
                + utf8Length(traceId)
                + utf8Length(payloadType)
                + Math.max(0, payloadLength);
    }

    private int utf8Length(String value) {
        return value == null || value.isEmpty() ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
    }

    private void handleDisconnect(String reason) {
        synchronized (connectionLock) {
            if (!started && !registryConnection.isConnected()) {
                return;
            }
            started = false;
            connectionGeneration.incrementAndGet();
            shutdownExecutor(heartbeatExecutor);
            closeConnectionResources();
        }
        log.warn(reason);
        scheduleReconnect();
    }

    private void sendHeartbeatSafely() {
        if (!started) {
            return;
        }
        try {
            if (System.nanoTime() - lastRegistryContact > TimeUnit.MILLISECONDS.toNanos(
                    Long.getLong("kynet.registry.responseTimeoutMs", 5000L))) {
                handleDisconnect("Registry heartbeat response timed out");
                return;
            }
            sendFrame(MessageType.HEARTBEAT, registrationGeneration.get(), 0, 0, 0, null, null, null, null, 0, 0);
        } catch (RegistryConnection.SendException e) {
            if (e.fatal() && started) {
                handleDisconnect("Registry heartbeat failed: " + e.getMessage());
            } else {
                log.debug("Registry heartbeat skipped: {}", e.getMessage());
            }
        } catch (RuntimeException e) {
            if (started) {
                handleDisconnect("Registry heartbeat failed: " + e.getMessage());
            }
        }
    }

    private void startRequestMaintenance() {
        if (!requestMaintenanceStarted.compareAndSet(false, true)) {
            return;
        }
        requestMaintenanceExecutor.scheduleAtFixedRate(this::maintainPendingRequestsSafely,
                RPC_TIMEOUT_SCAN_INTERVAL_MILLIS,
                RPC_TIMEOUT_SCAN_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private void maintainPendingRequestsSafely() {
        if (pendingRequests.isEmpty()) {
            return;
        }
        long nowNanos = System.nanoTime();
        pendingRequests.forEach((requestId, pending) -> {
            if (pending.isExpired(nowNanos)) {
                if (pendingRequests.remove(requestId, pending)) {
                    completeTimedOutRequest(pending);
                }
                return;
            }
            if (peerTransport.running() && pending.isRetryDue(nowNanos)) {
                sendPendingRequest(requestId, pending, true);
            }
        });
    }

    private void completeTimedOutRequest(PendingRequest pending) {
        String errorCode = pending.everSent() ? "RESULT_UNKNOWN" : "TIMEOUT";
        String message = pending.everSent()
                ? "RPC deadline expired after dispatch; execution result is unknown."
                : "RPC timeout before dispatch after " + pending.timeoutMillis() + " ms";
        completePendingFailure(pending, errorCode, message);
    }

    private void sendPendingRequest(long requestId, PendingRequest pending, boolean retry) {
        long nowNanos = System.nanoTime();
        if (!pending.beginAttempt(retry, nowNanos)) {
            return;
        }
        try {
            pending.markDispatchStarted();
            sendEncodedRpcRequest(requestId, pending.action(), pending.targetInstanceId(), pending.requestPayload());
            pending.markSent(System.nanoTime());
        } catch (RuntimeException e) {
            pending.markAttemptFailed();
            handlePendingSendFailure(requestId, pending, e);
        }
    }

    private boolean isRetryableRpcError(String errorCode) {
        return "NO_PROVIDER".equals(errorCode)
                || "PROVIDER_BACKPRESSURED".equals(errorCode)
                || "PROVIDER_UNAVAILABLE".equals(errorCode)
                || "BROKER_FORWARD_UNAVAILABLE".equals(errorCode)
                || "BROKER_DELIVERY_UNKNOWN".equals(errorCode)
                || "RPC_EXECUTOR_BUSY".equals(errorCode);
    }

    private void sendDirectPendingRequest(long requestId, PendingRequest pending,
                                          Object payload, InvokeOptions options) {
        try {
            pending.markDispatchStarted();
            sendRpcRequest(requestId, pending.action(), payload, options, false);
            pending.markSent(System.nanoTime());
        } catch (RuntimeException e) {
            handlePendingSendFailure(requestId, pending, e);
        }
    }

    private void sendPreparedPendingRequest(long requestId, PendingRequest pending,
                                            PreparedEnvelope envelope) {
        long nowNanos = System.nanoTime();
        if (pending.isIdempotent() && !pending.beginAttempt(false, nowNanos)) {
            return;
        }
        try {
            ensureDataStarted();
            envelope.writeRequestId(requestId);
            pending.markDispatchStarted();
            Endpoint endpoint = pending.selectedTarget;
            if (endpoint == null) {
                endpoint = resolveEndpoint(pending.action(), pending.targetInstanceId(),
                        pending.routingKey(requestId), pending.deadlineNanos);
                pending.selectedTarget = endpoint;
            }
            envelope.send(endpoint);
            pending.markSent(System.nanoTime());
        } catch (RuntimeException e) {
            pending.markAttemptFailed();
            handlePendingSendFailure(requestId, pending, e);
        }
    }

    private void handlePendingSendFailure(long requestId, PendingRequest pending, RuntimeException failure) {
        if (pendingRequests.get(requestId) != pending) {
            return;
        }
        boolean willRetry = pending.scheduleRetry(System.nanoTime());
        log.warn("RPC send failed requestId={} action={} target={} everSent={} willRetry={} error={}",
                requestId, pending.action(),
                pending.selectedTarget == null ? "unresolved" : pending.selectedTarget.instanceId(),
                pending.everSent(), willRetry, failure.toString());
        if (willRetry) {
            if (!started) {
                scheduleReconnect();
            }
            return;
        }
        completePendingFailure(requestId, pending,
                pending.everSent() ? "RESULT_UNKNOWN" : "SEND_FAILED",
                pending.everSent()
                        ? "RPC send failed after an earlier dispatch; execution result is unknown."
                        : failure.getMessage());
    }

    private void completePendingFailure(long requestId, PendingRequest pending,
                                        String errorCode, String errorMessage) {
        if (pendingRequests.remove(requestId, pending)) {
            completePendingFailure(pending, errorCode, errorMessage);
        }
    }

    private void completePendingFailure(PendingRequest pending, String errorCode, String errorMessage) {
        if (pending.fastCallback() != null) {
            notifyFastRpcFailure(pending.fastCallback(), errorCode, errorMessage);
            return;
        }
        pending.future().complete(ActionResult.error(
                errorCode,
                errorMessage,
                pending.attachmentData(),
                false));
    }

    private void failAllPendingRequests(String unsentErrorCode, String message) {
        pendingRequests.forEach((requestId, pending) -> completePendingFailure(
                requestId,
                pending,
                pending.everSent() ? "RESULT_UNKNOWN" : unsentErrorCode,
                pending.everSent()
                        ? "Service node closed after RPC dispatch; execution result is unknown."
                        : message));
    }

    private void ensureConnected() {
        if (closed) {
            throw new IllegalStateException("Service node is closed.");
        }
        if (started) {
            return;
        }
        connectOrThrow();
    }

    private void scheduleReconnect() {
        if (closed || reconnectExecutor.isShutdown()) {
            return;
        }
        long delay;
        synchronized (connectionLock) {
            if (started || reconnectScheduled || closed) {
                return;
            }
            reconnectScheduled = true;
            long ceiling = RECONNECT_DELAY_MILLIS;
            for (int i = 0; i < Math.min(reconnectAttempt++, 8) && ceiling < MAX_RECONNECT_DELAY_MILLIS; i++) {
                ceiling = ceiling > MAX_RECONNECT_DELAY_MILLIS / 2
                        ? MAX_RECONNECT_DELAY_MILLIS : ceiling * 2;
            }
            delay = ThreadLocalRandom.current().nextLong(Math.max(1L, ceiling / 2), ceiling + 1);
        }
        reconnectExecutor.schedule(this::reconnectSafely, delay, TimeUnit.MILLISECONDS);
    }

    private void reconnectSafely() {
        synchronized (connectionLock) {
            reconnectScheduled = false;
        }
        if (closed || started) {
            return;
        }
        try {
            connectOrThrow();
        } catch (RuntimeException e) {
            log.debug("Registry reconnect attempt failed: {}", e.getMessage());
            scheduleReconnect();
        }
    }

    private void addSeedRegistryEndpoint(String host, int port) {
        if (host == null || host.isBlank() || port <= 0) {
            return;
        }
        RegistryEndpoint endpoint = new RegistryEndpoint(host, port);
        seedRegistryEndpoints.add(endpoint);
        synchronized (topologyLock) {
            if (!registryEndpoints.contains(endpoint)) {
                List<RegistryEndpoint> next = new ArrayList<>(registryEndpoints);
                next.add(endpoint);
                registryEndpoints = List.copyOf(next);
            }
        }
    }

    private void closeConnectionResources() {
        registryConnection.disconnect();
    }

    private void shutdownExecutor(ExecutorService executor) {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private static long rpcRoutingKey(long sessionId, long userId, long fallbackRequestId) {
        if (sessionId != 0L) {
            return sessionId;
        }
        if (userId != 0L) {
            return userId;
        }
        return fallbackRequestId;
    }

    @FunctionalInterface
    public interface FastOneWayHandler {
        void onOneWay(int action, long requestId, long userId, int gatewayInstanceId,
                      DirectBuffer bodyBuffer, int bodyOffset, int bodyLength);
    }

    public interface FastRpcCallback {
        void onResponse(boolean success, RpcWireCodec.RpcResponseView response);

        default void onFailure(String errorCode, String errorMessage) {
        }
    }

    /**
     * Holds a pre-encoded frame that already reserves space for the direct envelope. For an
     * explicit target the envelope prefix is constant, so it is written once and reused across
     * sends; a zero target can be rerouted to another provider and keeps the per-message path.
     */
    private final class PreparedEnvelope {
        private final UnsafeBuffer buffer;
        private final int frameOffset;
        private final int frameLength;
        private final int targetInstanceId;
        private Endpoint initializedTarget;

        private PreparedEnvelope(UnsafeBuffer buffer, int frameOffset, int frameLength, int targetInstanceId) {
            this.buffer = buffer;
            this.frameOffset = frameOffset;
            this.frameLength = frameLength;
            this.targetInstanceId = targetInstanceId;
        }

        private void writeRequestId(long requestId) {
            buffer.putLong(frameOffset + FrameCodec.FIXED_HEADER_LENGTH, requestId);
        }

        private void send(Endpoint endpoint) {
            if (targetInstanceId != 0) {
                // Callers cache the resolved Endpoint, so an identity check is enough here;
                // a directory snapshot that recreates an equal Endpoint merely re-initializes.
                if (initializedTarget != endpoint) {
                    peerTransport.initializePreparedEnvelope(endpoint, buffer, frameOffset, frameLength);
                    initializedTarget = endpoint;
                }
                peerTransport.sendPreparedInitialized(endpoint, buffer, frameOffset, frameLength);
            } else {
                peerTransport.sendPrepared(endpoint, buffer, frameOffset, frameLength);
            }
        }
    }

    public final class FastOneWaySender {
        private final int action;
        private final int targetInstanceId;
        private final long configuredSessionId;
        private final long configuredUserId;
        private final long configuredRoutingKey;
        private final UnsafeBuffer frameBuffer;
        private final int frameOffset;
        private final int frameLength;
        private final int rpcPayloadOffset;
        private final PreparedEnvelope envelope;
        private final RequestIdSequence requestIds = new RequestIdSequence();
        private final boolean cacheRoute;
        private boolean configuredSessionUser = true;
        private Endpoint cachedEndpoint;
        private long cachedDirectoryVersion = Long.MIN_VALUE;

        private FastOneWaySender(int action, Object payload, InvokeOptions options) {
            this.action = action;
            this.targetInstanceId = options.getTargetInstanceId();
            this.configuredSessionId = options.getSessionId();
            this.configuredUserId = options.getUserId();
            this.configuredRoutingKey = rpcRoutingKey(options.getSessionId(), options.getUserId(), 0L);
            this.cacheRoute = configuredRoutingKey != 0L;
            byte[] body = ProtobufPayloadCodec.encode(payload);
            DirectBuffer bodyBuffer = body == null ? null : new UnsafeBuffer(body);
            int estimatedPayloadSize = 152
                    + (options.getAttachmentData() == null ? 0 : options.getAttachmentData().length)
                    + (body == null ? 0 : body.length);
            UnsafeBuffer payloadBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Math.max(64, estimatedPayloadSize)));
            int payloadLength = RpcWireCodec.encodeRequest(
                    payloadBuffer,
                    0,
                    options.getSessionId(),
                    options.getUserId(),
                    options.getGatewayInstanceId(),
                    options.getAttachmentData(),
                    options.getTimeout().toMillis(),
                    true,
                    bodyBuffer,
                    0,
                    body == null ? 0 : body.length);
            this.frameOffset = peerTransport.envelopeOverhead();
            this.frameBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(
                    frameOffset + estimateFrameSize(null, null, null, payloadLength)));
            this.frameLength = FrameCodec.encode(
                    MessageType.RPC_REQUEST,
                    action,
                    instanceId,
                    options.getTargetInstanceId(),
                    0,
                    0L,
                    null,
                    null,
                    null,
                    payloadBuffer,
                    0,
                    payloadLength,
                    frameBuffer,
                    frameOffset);
            this.rpcPayloadOffset = FrameCodec.decodeView(frameBuffer, frameOffset, frameLength).payloadOffset();
            this.envelope = new PreparedEnvelope(frameBuffer, frameOffset, frameLength, targetInstanceId);
        }

        public void send() {
            // The constructor already encoded the configured session/user; only restore them
            // if a previous send(long, long) overwrote the shared frame buffer.
            if (!configuredSessionUser) {
                frameBuffer.putLong(rpcPayloadOffset, configuredSessionId);
                frameBuffer.putLong(rpcPayloadOffset + Long.BYTES, configuredUserId);
                configuredSessionUser = true;
            }
            sendWithRoutingKey(configuredRoutingKey, cacheRoute);
        }

        public void send(long sessionId, long userId) {
            frameBuffer.putLong(rpcPayloadOffset, sessionId);
            frameBuffer.putLong(rpcPayloadOffset + Long.BYTES, userId);
            configuredSessionUser = false;
            sendWithRoutingKey(rpcRoutingKey(sessionId, userId, 0L), false);
        }

        private void sendWithRoutingKey(long configuredOrDynamicRoutingKey, boolean cacheRoute) {
            ensureDataStarted();
            long requestId = requestIds.next();
            frameBuffer.putLong(frameOffset + FrameCodec.FIXED_HEADER_LENGTH, requestId);
            long routingKey = configuredOrDynamicRoutingKey == 0L ? requestId : configuredOrDynamicRoutingKey;
            Endpoint endpoint;
            if (cacheRoute) {
                long directoryVersion = directory.version();
                endpoint = cachedEndpoint;
                if (endpoint == null || directoryVersion != cachedDirectoryVersion) {
                    endpoint = resolveEndpoint(action, targetInstanceId, routingKey);
                    cachedEndpoint = endpoint;
                    cachedDirectoryVersion = directoryVersion;
                }
            } else {
                endpoint = resolveEndpoint(action, targetInstanceId, routingKey);
            }
            try {
                envelope.send(endpoint);
            } catch (IllegalStateException e) {
                if (e.getMessage() == null || !e.getMessage().startsWith("NO_PROVIDER")) {
                    throw e;
                }
                endpoint = resolveEndpoint(action, targetInstanceId, routingKey);
                if (cacheRoute) {
                    cachedEndpoint = endpoint;
                    cachedDirectoryVersion = directory.version();
                }
                envelope.send(endpoint);
            }
        }
    }

    private Endpoint resolveEndpoint(int action, int targetInstanceId, long routingKey) {
        return resolveEndpoint(action, targetInstanceId, routingKey, Long.MAX_VALUE);
    }

    private Endpoint resolveEndpoint(int action, int targetInstanceId, long routingKey, long requestDeadlineNanos) {
        Endpoint endpoint = directory.select(action, targetInstanceId, routingKey);
        if (endpoint == null) {
            long discoveryDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                    Math.max(0L, Long.getLong("kynet.direct.discoveryWaitMs", 1000L)));
            long deadline = Math.min(discoveryDeadline, requestDeadlineNanos);
            do {
                endpoint = directory.select(action, targetInstanceId, routingKey);
                if (endpoint != null || !started || System.nanoTime() >= deadline) break;
                java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            } while (!closed);
            if (endpoint == null) {
                throw new IllegalStateException("NO_PROVIDER: no direct provider for action " + action);
            }
        }
        return endpoint;
    }

    public final class FastRpcSender {
        private final int action;
        private final InvokeOptions options;
        private final byte[] requestPayload;
        private final UnsafeBuffer frameBuffer;
        private final int frameOffset;
        private final int frameLength;
        private final PreparedEnvelope envelope;
        private final RequestIdSequence requestIds = new RequestIdSequence();

        private FastRpcSender(int action, Object payload, InvokeOptions options) {
            this.action = action;
            this.options = options;
            this.requestPayload = encodeRpcRequest(payload, options, false);
            UnsafeBuffer payloadBuffer = new UnsafeBuffer(requestPayload);
            int payloadLength = requestPayload.length;
            this.frameOffset = peerTransport.envelopeOverhead();
            this.frameBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(
                    frameOffset + estimateFrameSize(null, null, null, payloadLength)));
            this.frameLength = FrameCodec.encode(
                    MessageType.RPC_REQUEST,
                    action,
                    instanceId,
                    options.getTargetInstanceId(),
                    0,
                    0L,
                    null,
                    null,
                    null,
                    payloadBuffer,
                    0,
                    payloadLength,
                    frameBuffer,
                    frameOffset);
            this.envelope = new PreparedEnvelope(frameBuffer, frameOffset, frameLength,
                    options.getTargetInstanceId());
        }

        public CompletableFuture<ActionResult> invokeAsync() {
            ensureDataStarted();
            long requestId = requestIds.next();
            CompletableFuture<ActionResult> future = new CompletableFuture<>();
            PendingRequest pending = PendingRequest.future(action, future,
                    responseDecoders.get(action), options, requestPayload);
            pendingRequests.put(requestId, pending);
            sendPreparedPendingRequest(requestId, pending, envelope);
            return future;
        }

        public void invoke(FastRpcCallback callback) {
            if (callback == null) {
                throw new IllegalArgumentException("callback must not be null");
            }
            if (!peerTransport.running()) {
                try {
                    ensureDataStarted();
                } catch (RuntimeException e) {
                    callback.onFailure("BROKER_DISCONNECTED", e.getMessage());
                    return;
                }
            }
            long requestId = requestIds.next();
            PendingRequest pending = PendingRequest.fastCallback(
                    action, callback, options, requestPayload);
            pendingRequests.put(requestId, pending);
            sendPreparedPendingRequest(requestId, pending, envelope);
        }
    }

    private long nextRequestId() {
        long[] slot = requestIdStripe.get();
        long id = slot[0] + 1;
        if ((id & 0xFFFF_FFFFL) == 0L) {
            id = (long) requestIdStripeSeq.getAndIncrement() << 32;
        }
        slot[0] = id;
        return id;
    }

    /**
     * Request-id sequence owned by one prepared sender. Prepared senders are single-threaded
     * by contract, so this avoids a ThreadLocal lookup per message while keeping the same
     * client-wide uniqueness guarantee.
     */
    private final class RequestIdSequence {
        private long current = (long) requestIdStripeSeq.getAndIncrement() << 32;

        private long next() {
            long id = current + 1;
            if ((id & 0xFFFF_FFFFL) == 0L) {
                id = (long) requestIdStripeSeq.getAndIncrement() << 32;
            }
            current = id;
            return id;
        }
    }

    private void sendRpcRequest(long requestId, int action, Object payload, InvokeOptions options, boolean oneWay) {
        byte[] body = ProtobufPayloadCodec.encode(payload);
        DirectBuffer bodyBuffer = body == null ? null : new UnsafeBuffer(body);
        int estimatedSize = 153
                + (options.getAttachmentData() == null ? 0 : options.getAttachmentData().length)
                + (body == null ? 0 : body.length);
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(Math.max(64, estimatedSize));
        try {
            int payloadLength = RpcWireCodec.encodeRequest(
                    payloadBuffer, 0,
                    options.getSessionId(),
                    options.getUserId(),
                    options.getGatewayInstanceId(),
                    options.getAttachmentData(),
                    options.getTimeout().toMillis(),
                    oneWay,
                    bodyBuffer, 0, body == null ? 0 : body.length,
                    !oneWay && options.isIdempotent());
            sendFrame(MessageType.RPC_REQUEST, requestId, action, options.getTargetInstanceId(), 0,
                    null, null, null, payloadBuffer, 0, payloadLength);
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    private byte[] encodeRpcRequest(Object payload, InvokeOptions options, boolean oneWay) {
        byte[] body = ProtobufPayloadCodec.encode(payload);
        DirectBuffer bodyBuffer = body == null ? null : new UnsafeBuffer(body);
        int estimatedSize = 153
                + (options.getAttachmentData() == null ? 0 : options.getAttachmentData().length)
                + (body == null ? 0 : body.length);
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(Math.max(64, estimatedSize));
        try {
            int payloadLength = RpcWireCodec.encodeRequest(
                    payloadBuffer, 0,
                    options.getSessionId(),
                    options.getUserId(),
                    options.getGatewayInstanceId(),
                    options.getAttachmentData(),
                    options.getTimeout().toMillis(),
                    oneWay,
                    bodyBuffer, 0, body == null ? 0 : body.length,
                    !oneWay && options.isIdempotent());
            byte[] encoded = new byte[payloadLength];
            payloadBuffer.getBytes(0, encoded);
            return encoded;
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    private void sendEncodedRpcRequest(long requestId, int action, int targetInstanceId, byte[] requestPayload) {
        DirectBuffer payloadBuffer = new UnsafeBuffer(requestPayload);
        sendFrame(MessageType.RPC_REQUEST, requestId, action, targetInstanceId, 0,
                null, null, null, payloadBuffer, 0, requestPayload.length);
    }

    private InvokeOptions resolveOptions(InvokeOptions options) {
        return options == null ? InvokeOptions.defaults() : options;
    }

    private void sendRpcSuccessResponse(long requestId, String traceId, int action, int targetInstanceId, byte[] body,
                                        boolean attachmentUpdated,
                                        byte[] attachmentData, long targetIncarnation) {
        int estimatedSize = 124
                + (attachmentData == null ? 0 : attachmentData.length)
                + (body == null ? 0 : body.length);
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(Math.max(64, estimatedSize));
        try {
            int payloadLength = RpcWireCodec.encodeResponse(
                    payloadBuffer, 0, true, null, null,
                    attachmentUpdated, attachmentData, body);
            sendDirectResponse(requestId, action, targetInstanceId, targetIncarnation, traceId, payloadBuffer, payloadLength);
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    private void sendRpcErrorResponse(long requestId, String traceId, int action, int targetInstanceId,
                                      String errorCode, String errorMessage,
                                      boolean attachmentUpdated,
                                      byte[] attachmentData, long targetIncarnation) {
        int estimatedSize = 252
                + (attachmentData == null ? 0 : attachmentData.length);
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(estimatedSize);
        try {
            int payloadLength = RpcWireCodec.encodeResponse(
                    payloadBuffer, 0, false, errorCode, errorMessage,
                    attachmentUpdated, attachmentData, (byte[]) null);
            sendDirectResponse(requestId, action, targetInstanceId, targetIncarnation, traceId, payloadBuffer, payloadLength);
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    private void sendDirectResponse(long requestId, int action, int target, long incarnation, String traceId,
                                    DirectBuffer payload, int payloadLength) {
        Endpoint endpoint = peerTransport.replyEndpoint(target);
        if (endpoint == null || incarnation != 0 && endpoint.incarnation() != incarnation) {
            throw new IllegalStateException("Reply destination incarnation is no longer available");
        }
        UnsafeBuffer buffer = BufferPool.acquireShared(estimateFrameSize(null, traceId, null, payloadLength));
        try {
            int length = FrameCodec.encode(MessageType.RPC_RESPONSE, action, instanceId, target, 0, requestId,
                    null, traceId, null, payload, 0, payloadLength, buffer, 0);
            peerTransport.send(endpoint, buffer, 0, length);
        } finally { BufferPool.releaseShared(buffer); }
    }

    private Object decodeResponseBody(PendingRequest pending, RpcWireCodec.RpcResponseView response) {
        PayloadDecoder<?> decoder = pending.decoder();
        if (decoder != null) {
            return decoder.decode(response.bodyBuffer(), response.bodyOffset(), response.bodyLength());
        }
        return response.bodyLength() == 0 ? null : response.copyBody();
    }

    private void notifyFastRpcFailure(FastRpcCallback callback, String errorCode, String errorMessage) {
        try {
            callback.onFailure(errorCode, errorMessage);
        } catch (RuntimeException e) {
            log.warn("Fast RPC failure callback failed: {}", e.getMessage());
        }
    }

    static final class PendingRequest {
        private final int action;
        private final CompletableFuture<ActionResult> future;
        private final PayloadDecoder<?> decoder;
        private final FastRpcCallback fastCallback;
        private final long deadlineNanos;
        private final long timeoutMillis;
        private final byte[] attachmentData;
        private final int targetInstanceId;
        private final long configuredRoutingKey;
        private final byte[] requestPayload;
        private final boolean idempotent;
        private final int maxAttempts;
        private final long retryBackoffNanos;
        private int attempts;
        private boolean attemptInProgress;
        private boolean retryPending;
        private long retryAtNanos;
        private volatile boolean everSent;
        private volatile Endpoint selectedTarget;

        private PendingRequest(int action, CompletableFuture<ActionResult> future, PayloadDecoder<?> decoder,
                               FastRpcCallback fastCallback, InvokeOptions options, byte[] requestPayload) {
            this.action = action;
            this.future = future;
            this.decoder = decoder;
            this.fastCallback = fastCallback;
            this.deadlineNanos = deadlineNanos(options);
            this.timeoutMillis = options.getTimeout().toMillis();
            this.attachmentData = options.getAttachmentData();
            this.targetInstanceId = options.getTargetInstanceId();
            this.configuredRoutingKey = rpcRoutingKey(options.getSessionId(), options.getUserId(), 0L);
            this.requestPayload = requestPayload;
            this.idempotent = options.isIdempotent();
            this.maxAttempts = 1 + options.getMaxRetries();
            this.retryBackoffNanos = options.getRetryBackoff().toNanos();
        }

        private static PendingRequest future(int action, CompletableFuture<ActionResult> future,
                                             PayloadDecoder<?> decoder, InvokeOptions options,
                                             byte[] requestPayload) {
            return new PendingRequest(action, future, decoder, null, options, requestPayload);
        }

        private static PendingRequest fastCallback(int action, FastRpcCallback callback, InvokeOptions options,
                                                   byte[] requestPayload) {
            return new PendingRequest(action, null, null, callback, options, requestPayload);
        }

        private static long deadlineNanos(InvokeOptions options) {
            return System.nanoTime() + Math.max(1L, options.getTimeout().toNanos());
        }

        private boolean isExpired(long nowNanos) {
            return nowNanos - deadlineNanos >= 0;
        }

        private synchronized boolean beginAttempt(boolean retry, long nowNanos) {
            if (isExpired(nowNanos) || attemptInProgress || attempts >= maxAttempts) {
                return false;
            }
            if (retry && (!retryPending || nowNanos - retryAtNanos < 0L)) {
                return false;
            }
            if (!retry && attempts != 0) {
                return false;
            }
            retryPending = false;
            attemptInProgress = true;
            attempts++;
            return true;
        }

        private void markSent(long nowNanos) {
            if (!idempotent) {
                everSent = true;
                return;
            }
            synchronized (this) {
                everSent = true;
                attemptInProgress = false;
                if (attempts < maxAttempts && !isExpired(nowNanos)) {
                    scheduleRetryAt(nowNanos);
                }
            }
        }

        private void markDispatchStarted() {
            everSent = true;
        }

        private void markAttemptFailed() {
            if (!idempotent) {
                return;
            }
            synchronized (this) {
                attemptInProgress = false;
            }
        }

        private synchronized boolean scheduleRetry(long nowNanos) {
            if (!idempotent || attempts >= maxAttempts || isExpired(nowNanos)) {
                return false;
            }
            scheduleRetryAt(nowNanos);
            return true;
        }

        private void scheduleRetryAt(long nowNanos) {
            long retryAt = nowNanos + retryBackoffNanos;
            retryAtNanos = retryAt < 0L ? Long.MAX_VALUE : retryAt;
            retryPending = true;
        }

        private synchronized boolean isRetryDue(long nowNanos) {
            return retryPending && !attemptInProgress && nowNanos - retryAtNanos >= 0L;
        }

        private boolean everSent() {
            return everSent;
        }

        private boolean isIdempotent() {
            return idempotent;
        }

        private int action() {
            return action;
        }

        private CompletableFuture<ActionResult> future() {
            return future;
        }

        private PayloadDecoder<?> decoder() {
            return decoder;
        }

        private FastRpcCallback fastCallback() {
            return fastCallback;
        }

        private long timeoutMillis() {
            return timeoutMillis;
        }

        private byte[] attachmentData() {
            return attachmentData;
        }

        private int targetInstanceId() {
            return targetInstanceId;
        }

        private long routingKey(long requestId) {
            return configuredRoutingKey == 0L ? requestId : configuredRoutingKey;
        }

        private byte[] requestPayload() {
            return requestPayload;
        }
    }

    private static String parseHost(String addr) {
        int index = addr.lastIndexOf(':');
        if (index <= 0) throw new IllegalArgumentException("Address must be host:port");
        return addr.substring(0, index);
    }

    private static int parsePort(String addr) {
        int index = addr.lastIndexOf(':');
        if (index <= 0) throw new IllegalArgumentException("Address must be host:port");
        return Integer.parseInt(addr.substring(index + 1));
    }

    private static RegistryEndpoint firstAddress(List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Registry seed address list must not be empty");
        }
        String address = addresses.get(0);
        return new RegistryEndpoint(parseHost(address), parsePort(address));
    }

    private record RegistryEndpoint(String host, int port) {
        @Override
        public String toString() {
            return host + ":" + port;
        }
    }
}

