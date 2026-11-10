package cn.itsub.kynet.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

final class RegistryClusterService {
    private static final Logger log = LoggerFactory.getLogger(RegistryClusterService.class);
    private static final long GOSSIP_ASSEMBLY_TTL_MILLIS = 30_000L;
    private static final int MAX_REPLAY_SOURCES = ClusterMessageCodec.MAX_MEMBERS * 8;
    private static final int AUTHENTICATED_REPLAY_WINDOW_CAPACITY = 8_192;

    private final RegistryNodeConfig config;
    private final RegistryMemberTable memberTable;
    private final RegistryClusterRouteTable routeTable;
    private final RegistryServer registryServer;
    private final Object sendLock = new Object();
    private final Object gossipLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private final LongAdder sendFailures = new LongAdder();
    private final LongAdder authenticatedReplayRejected = new LongAdder();
    private final Map<ReplaySource, AuthenticatedReplayWindow> authenticatedReplayWindows =
            new ConcurrentHashMap<>();
    private final Map<GossipKey, GossipAssembly> gossipAssemblies = new ConcurrentHashMap<>();
    private ScheduledExecutorService executor;
    private DatagramChannel discoveryChannel;
    private DatagramChannel clusterChannel;
    private long lastGossipedRouteVersion = -1L;
    private long lastRouteSnapshotMillis;
    private volatile long startedAtMillis;
    private volatile boolean readyForClients;

    RegistryClusterService(RegistryNodeConfig config, RegistryClusterRouteTable routeTable, RegistryServer registryServer) {
        this.config = config;
        this.routeTable = routeTable;
        this.registryServer = registryServer;
        this.memberTable = new RegistryMemberTable(config);
    }

    void start() {
        if (!config.clusterEnabled() || !running.compareAndSet(false, true)) return;
        startedAtMillis = System.currentTimeMillis();
        try {
            discoveryChannel = openDiscoveryChannel();
            clusterChannel = openClusterChannel();
            executor = Executors.newScheduledThreadPool(4, task -> {
                Thread thread = new Thread(task, "registry-discovery-" + config.nodeId());
                thread.setDaemon(true);
                return thread;
            });
            executor.execute(this::discoveryReadLoop);
            executor.execute(this::clusterReadLoop);
            executor.scheduleAtFixedRate(this::sendDiscoverySafely, 0, config.discoveryIntervalMillis(), TimeUnit.MILLISECONDS);
            executor.scheduleAtFixedRate(this::sendGossipSafely, config.gossipIntervalMillis(), config.gossipIntervalMillis(), TimeUnit.MILLISECONDS);
            executor.scheduleAtFixedRate(this::detectFailuresSafely, config.gossipIntervalMillis(), config.gossipIntervalMillis(), TimeUnit.MILLISECONDS);
            executor.scheduleAtFixedRate(this::cleanupTransientStateSafely, 1000, 1000, TimeUnit.MILLISECONDS);
            log.info("Registry discovery started nodeId={} epoch={} client={}:{} cluster={}:{}", config.nodeId(), config.epoch(),
                    config.clientHost(), config.clientPort(), config.clusterHost(), config.clusterPort());
        } catch (IOException | RuntimeException e) {
            running.set(false);
            if (executor != null) executor.shutdownNow();
            closeQuietly(discoveryChannel);
            closeQuietly(clusterChannel);
            throw new IllegalStateException("Failed to start registry discovery", e);
        }
    }

    synchronized void stop() {
        stop(true);
    }

    synchronized void stopImmediately() {
        stop(false);
    }

    private void stop(boolean announceDeparture) {
        if (!running.get()) {
            return;
        }
        try {
            if (announceDeparture) {
                memberTable.markLeft();
                sendGossipSafely(true);
            }
        } finally {
            running.set(false);
            if (executor != null) {
                executor.shutdownNow();
            }
            authenticatedReplayWindows.clear();
            gossipAssemblies.clear();
            closeQuietly(discoveryChannel);
            closeQuietly(clusterChannel);
        }
    }

    void onRoutesChanged() {
        if (running.get()) {
            sendGossipSafely(true);
        }
    }

    boolean isRouteAlive(ClusterRoute route) {
        return route != null && memberTable.isAlive(route.ownerNodeId(), route.ownerEpoch());
    }

    synchronized boolean isReadyForClients() {
        if (!config.clusterEnabled()) return true;
        if (!running.get()) return false;
        if (readyForClients) return true;
        long grace = Math.max(0L, Long.getLong("kynet.registry.joinGraceMs",
                Math.max(200L, config.discoveryIntervalMillis() * 2)));
        if (System.currentTimeMillis() - startedAtMillis < grace) return false;
        for (RegistryMember member : memberTable.snapshots()) {
            if (!config.nodeId().equals(member.nodeId()) && member.state() == MemberState.ALIVE
                    && !routeTable.hasRemoteSnapshot(member.nodeId(), member.epoch())) return false;
        }
        readyForClients = true;
        log.info("Registry ready for registrations nodeId={} members={}", config.nodeId(), memberTable.snapshots().size());
        return true;
    }

    List<cn.itsub.kynet.common.protocol.RegistryTopologyCodec.Endpoint> topologyEndpoints() {
        return memberTable.topologyEndpoints();
    }

    String nodeId() {
        return config.nodeId();
    }

    RegistryClusterMetrics metrics() {
        int alive = 0;
        int suspect = 0;
        int unavailable = 0;
        for (RegistryMember member : memberTable.snapshots()) {
            switch (member.state()) {
                case ALIVE -> alive++;
                case SUSPECT -> suspect++;
                case DEAD, LEFT -> unavailable++;
            }
        }
        return new RegistryClusterMetrics(sendFailures.sum(), authenticatedReplayRejected.sum(),
                gossipAssemblies.size(), alive, suspect, unavailable);
    }

    private DatagramChannel openDiscoveryChannel() throws IOException {
        DatagramChannel channel = DatagramChannel.open();
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_BROADCAST, true);
        channel.setOption(StandardSocketOptions.SO_RCVBUF, config.clusterSocketBufferBytes());
        channel.setOption(StandardSocketOptions.SO_SNDBUF, config.clusterSocketBufferBytes());
        channel.bind(new InetSocketAddress(config.discoveryBindHost(), config.discoveryPort()));
        channel.configureBlocking(true);
        return channel;
    }

    private DatagramChannel openClusterChannel() throws IOException {
        DatagramChannel channel = DatagramChannel.open();
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_RCVBUF, config.clusterSocketBufferBytes());
        channel.setOption(StandardSocketOptions.SO_SNDBUF, config.clusterSocketBufferBytes());
        channel.bind(new InetSocketAddress(config.clusterBindHost(), config.clusterPort()));
        channel.configureBlocking(true);
        return channel;
    }

    private void discoveryReadLoop() {
        readLoop(discoveryChannel, true, "discovery");
    }

    private void clusterReadLoop() {
        readLoop(clusterChannel, false, "cluster");
    }

    private void readLoop(DatagramChannel channel, boolean discoveryTransport, String label) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(ClusterMessageCodec.MAX_DATAGRAM_BYTES);
        while (running.get()) {
            try {
                buffer.clear();
                SocketAddress source = channel.receive(buffer);
                if (source instanceof InetSocketAddress inetSource) {
                    handleDatagram(buffer, inetSource, discoveryTransport);
                }
            } catch (IOException e) {
                if (running.get()) {
                    log.warn("Broker {} receive failed: {}", label, e.getMessage());
                }
            } catch (RuntimeException e) {
                if (running.get()) {
                    log.warn("Ignore invalid broker {} datagram: {}", label, e.getMessage());
                }
            }
        }
    }

    private void handleDatagram(ByteBuffer buffer, InetSocketAddress source, boolean discoveryTransport) {
        int length = buffer.position();
        if (length <= 0 || length > ClusterMessageCodec.MAX_DATAGRAM_BYTES) {
            return;
        }
        byte[] payload = new byte[length];
        buffer.flip();
        buffer.get(payload);
        ClusterMessageCodec.ClusterMessage message = ClusterMessageCodec.decode(config, payload, length);
        if (message == null || !config.clusterName().equals(message.clusterName())
                || config.nodeId().equals(message.fromNodeId())) {
            return;
        }
        if (message instanceof ClusterMessageCodec.DiscoveryMessage discovery) {
            if (discoveryTransport && acceptAuthenticatedMessage(message)) {
                handleDiscovery(discovery);
            }
            return;
        }
        if (discoveryTransport || !ensureExpectedClusterSource(message, source)) {
            return;
        }
        if (!acceptAuthenticatedMessage(message)) {
            return;
        }
        markMemberDirectlySeen(message.fromNodeId(), message.fromEpoch(), "cluster datagram");
        switch (message) {
            case ClusterMessageCodec.GossipMessage gossip -> handleGossip(gossip);
            default -> {
            }
        }
    }

    private void handleDiscovery(ClusterMessageCodec.DiscoveryMessage message) {
        if (message.clientPort() <= 0 || message.clusterPort() <= 0) return;
        RegistryMember previous = memberTable.member(message.fromNodeId());
        if (memberTable.mergeDiscovery(message)) {
            RegistryMember current = memberTable.member(message.fromNodeId());
            log.info("Registry member changed via discovery nodeId={} epoch={} state={} previousState={}",
                    current.nodeId(), current.epoch(), current.state(),
                    previous == null ? "UNKNOWN" : previous.state());
            registryServer.onClusterMembershipChanged();
            sendGossipSafely(true);
        }
    }

    private void markMemberDirectlySeen(String nodeId, long epoch, String transport) {
        if (!memberTable.markDirectlySeen(nodeId, epoch)) {
            return;
        }
        log.info("Registry member revived by authenticated {} nodeId={} epoch={}",
                transport, nodeId, epoch);
        registryServer.onClusterMembershipChanged();
        sendGossipSafely(true);
    }

    private boolean acceptAuthenticatedMessage(ClusterMessageCodec.ClusterMessage message) {
        ReplaySource source = new ReplaySource(
                message.fromNodeId(), message.fromEpoch(), replayLane(message));
        AuthenticatedReplayWindow existing = authenticatedReplayWindows.get(source);
        if (existing == null && authenticatedReplayWindows.size() >= MAX_REPLAY_SOURCES) {
            authenticatedReplayRejected.increment();
            return false;
        }
        AuthenticatedReplayWindow window = authenticatedReplayWindows.computeIfAbsent(
                source, ignored -> new AuthenticatedReplayWindow(
                        AUTHENTICATED_REPLAY_WINDOW_CAPACITY));
        AuthenticatedReplayWindow.Admission admission = window.admit(
                message.nonce(), System.currentTimeMillis());
        if (admission == AuthenticatedReplayWindow.Admission.FIRST_SEEN) {
            return true;
        }
        authenticatedReplayRejected.increment();
        return false;
    }

    private static int replayLane(ClusterMessageCodec.ClusterMessage message) {
        return switch (message) {
            case ClusterMessageCodec.DiscoveryMessage ignored -> 1;
            case ClusterMessageCodec.GossipMessage ignored -> 2;
        };
    }

    private void handleGossip(ClusterMessageCodec.GossipMessage message) {
        boolean membersChanged = memberTable.mergeRumors(message.members());
        boolean routesChanged = mergeGossipRoutes(message);
        if (membersChanged) {
            log.info("Registry membership changed via gossip fromNodeId={} members={}",
                    message.fromNodeId(), memberTable.snapshots());
            removeDeadMemberRoutes();
            registryServer.onClusterMembershipChanged();
        }
        if (routesChanged) {
            log.info("Registry route snapshot installed ownerNodeId={} ownerEpoch={} version={} routes={}",
                    message.fromNodeId(), message.fromEpoch(), message.routeSnapshotVersion(),
                    message.routes().size());
        }
    }

    private boolean mergeGossipRoutes(ClusterMessageCodec.GossipMessage message) {
        if (message.routeSnapshotVersion() < 0) {
            return false;
        }
        for (ClusterRoute route : message.routes()) {
            if (!message.fromNodeId().equals(route.ownerNodeId())
                    || message.fromEpoch() != route.ownerEpoch()) {
                return false;
            }
        }
        if (message.chunkCount() == 1) {
            return routeTable.replaceRemoteSnapshot(message.fromNodeId(), message.fromEpoch(),
                    message.routeSnapshotVersion(), message.routes());
        }
        GossipKey key = new GossipKey(
                message.fromNodeId(), message.fromEpoch(), message.routeSnapshotVersion());
        GossipAssembly assembly = gossipAssemblies.computeIfAbsent(key,
                ignored -> new GossipAssembly(message.chunkCount(), System.currentTimeMillis()));
        List<ClusterRoute> completed = assembly.add(
                message.chunkIndex(), message.chunkCount(), message.routes());
        if (completed == null) {
            return false;
        }
        gossipAssemblies.remove(key, assembly);
        return routeTable.replaceRemoteSnapshot(message.fromNodeId(), message.fromEpoch(),
                message.routeSnapshotVersion(), completed);
    }

    private void sendDiscoverySafely() {
        try {
            byte[] payload = ClusterMessageCodec.encodeDiscovery(config, memberTable.self());
            sendDatagram(discoveryChannel, payload, config.discoveryAddress(), config.discoveryPort());
        } catch (RuntimeException e) {
            if (running.get()) {
                log.warn("Registry discovery broadcast failed: {}", e.getMessage());
            }
        }
    }

    private void sendGossipSafely() {
        sendGossipSafely(false);
    }

    private void sendGossipSafely(boolean forceRoutes) {
        if (!running.get()) {
            return;
        }
        synchronized (gossipLock) {
            try {
                long now = System.currentTimeMillis();
                long routeVersion = routeTable.localSnapshotVersion();
                boolean includeRoutes = forceRoutes
                        || routeVersion != lastGossipedRouteVersion
                        || now - lastRouteSnapshotMillis >= config.routeSnapshotIntervalMillis();
                List<ClusterRoute> routes = includeRoutes
                        ? routeTable.localSnapshots()
                        : Collections.emptyList();
                long snapshotVersion = includeRoutes ? routeVersion : -1L;
                List<List<ClusterRoute>> chunks = chunkRoutes(routes);
                List<RegistryMember> members = memberTable.snapshots();
                boolean allSent = true;
                List<RegistryMember> targets = memberTable.gossipTargets();
                for (int chunkIndex = 0; chunkIndex < chunks.size(); chunkIndex++) {
                    byte[] payload = ClusterMessageCodec.encodeGossip(
                            config, members, chunks.get(chunkIndex), snapshotVersion,
                            chunkIndex, chunks.size());
                    for (RegistryMember member : targets) {
                        allSent &= sendDatagram(clusterChannel, payload, member.host(), member.clusterPort());
                    }
                }
                if (includeRoutes && allSent) {
                    lastGossipedRouteVersion = routeVersion;
                    lastRouteSnapshotMillis = now;
                }
            } catch (RuntimeException e) {
                if (running.get()) {
                    log.warn("Registry gossip failed: {}", e.getMessage());
                }
            }
        }
    }

    private List<List<ClusterRoute>> chunkRoutes(List<ClusterRoute> routes) {
        if (routes.isEmpty()) {
            return List.of(List.of());
        }
        int chunkSize = Math.min(config.gossipRoutesPerChunk(), ClusterMessageCodec.MAX_ROUTES_PER_CHUNK);
        int overhead = ClusterMessageCodec.encodeGossip(config, memberTable.snapshots(), List.of(), 0L, 0, 1).length;
        List<List<ClusterRoute>> chunks = new ArrayList<>();
        List<ClusterRoute> chunk = new ArrayList<>();
        int bytes = overhead;
        for (ClusterRoute route : routes) {
            int size = ClusterMessageCodec.encodedRouteSize(route);
            if (overhead + size > ClusterMessageCodec.MAX_DATAGRAM_BYTES) throw new IllegalArgumentException("Route exceeds gossip datagram budget");
            if (chunk.size() == chunkSize || bytes + size > ClusterMessageCodec.MAX_DATAGRAM_BYTES) {
                chunks.add(List.copyOf(chunk)); chunk.clear(); bytes = overhead;
            }
            chunk.add(route); bytes += size;
        }
        if (!chunk.isEmpty()) chunks.add(List.copyOf(chunk));
        return chunks;
    }

    private void detectFailuresSafely() {
        if (memberTable.detectFailures(System.currentTimeMillis())) {
            log.info("Registry membership changed after failure detection members={}",
                    memberTable.snapshots());
            removeDeadMemberRoutes();
            registryServer.onClusterMembershipChanged();
            sendGossipSafely(true);
        }
    }

    private void removeDeadMemberRoutes() {
        for (RegistryMember member : memberTable.snapshots()) {
            if (member.state() == MemberState.DEAD || member.state() == MemberState.LEFT) {
                routeTable.removeRoutesForNode(member.nodeId(), member.epoch());
            }
        }
    }

    private void cleanupTransientStateSafely() {
        long now = System.currentTimeMillis();
        authenticatedReplayWindows.forEach((source, observed) ->
                authenticatedReplayWindows.computeIfPresent(source, (ignored, current) ->
                        current == observed
                                && current.isExpired(now, config.protocolClockSkewMillis())
                                ? null
                                : current));
        gossipAssemblies.entrySet().removeIf(entry ->
                now - entry.getValue().createdAtMillis() >= GOSSIP_ASSEMBLY_TTL_MILLIS);
    }

    private boolean ensureExpectedClusterSource(ClusterMessageCodec.ClusterMessage message,
                                                InetSocketAddress source) {
        RegistryMember member = memberTable.member(message.fromNodeId());
        if (member != null
                && member.epoch() == message.fromEpoch()
                && member.clusterPort() == source.getPort()
                && sourceMatchesHost(source, member.host())) {
            return true;
        }
        if (!(message instanceof ClusterMessageCodec.GossipMessage gossip)) {
            return false;
        }
        RegistryMember sender = gossip.members().stream()
                .filter(candidate -> message.fromNodeId().equals(candidate.nodeId()))
                .filter(candidate -> message.fromEpoch() == candidate.epoch())
                .filter(candidate -> candidate.state() == MemberState.ALIVE)
                .findFirst()
                .orElse(null);
        if (sender == null
                || sender.clusterPort() != source.getPort()
                || !sourceMatchesHost(source, sender.host())) {
            return false;
        }
        boolean changed = memberTable.mergeRumors(List.of(sender));
        changed |= memberTable.markDirectlySeen(sender.nodeId(), sender.epoch());
        if (changed) {
            registryServer.onClusterMembershipChanged();
        }
        RegistryMember accepted = memberTable.member(message.fromNodeId());
        return accepted != null
                && accepted.epoch() == message.fromEpoch()
                && accepted.clusterPort() == source.getPort();
    }

    private boolean sourceMatchesHost(InetSocketAddress source, String host) {
        if (source == null || source.getAddress() == null || host == null || host.isBlank()) {
            return false;
        }
        try {
            InetAddress expected = InetAddress.getByName(host);
            return expected.equals(source.getAddress());
        } catch (IOException e) {
            return false;
        }
    }

    private boolean sendDatagram(DatagramChannel channel, byte[] payload, String host, int port) {
        if (channel == null || !channel.isOpen() || payload == null || payload.length == 0
                || payload.length > ClusterMessageCodec.MAX_DATAGRAM_BYTES) {
            return false;
        }
        try {
            synchronized (sendLock) {
                boolean sent = channel.send(ByteBuffer.wrap(payload), new InetSocketAddress(host, port)) == payload.length;
                if (!sent) sendFailures.increment();
                return sent;
            }
        } catch (IOException e) {
            sendFailures.increment();
            if (running.get()) {
                log.debug("Registry cluster send failed target={}:{} reason={}", host, port, e.getMessage());
            }
            return false;
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private record ReplaySource(String sourceNodeId, long sourceEpoch, int lane) {}

    private record GossipKey(String sourceNodeId, long sourceEpoch, long snapshotVersion) {
    }

    private static final class GossipAssembly {
        private final List<ClusterRoute>[] chunks;
        private final long createdAtMillis;
        private int received;

        @SuppressWarnings("unchecked")
        private GossipAssembly(int chunkCount, long createdAtMillis) {
            this.chunks = new List[chunkCount];
            this.createdAtMillis = createdAtMillis;
        }

        synchronized List<ClusterRoute> add(int chunkIndex, int chunkCount, List<ClusterRoute> routes) {
            if (chunkCount != chunks.length || chunkIndex < 0 || chunkIndex >= chunks.length) {
                return null;
            }
            if (chunks[chunkIndex] == null) {
                chunks[chunkIndex] = List.copyOf(routes);
                received++;
            }
            if (received != chunks.length) {
                return null;
            }
            List<ClusterRoute> combined = new ArrayList<>();
            for (List<ClusterRoute> chunk : chunks) {
                combined.addAll(Objects.requireNonNull(chunk));
            }
            return combined;
        }

        long createdAtMillis() {
            return createdAtMillis;
        }
    }
}
