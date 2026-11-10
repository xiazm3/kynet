package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.protocol.GatewayPushMessage;
import cn.itsub.kynet.common.protocol.PlayerBroadcastEnvelope;
import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.action.BaseAttachment;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import cn.itsub.kynet.framework.user.UserHook;
import cn.itsub.kynet.framework.user.UserSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class GatewayRuntime implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GatewayRuntime.class);
    private static final long DEFAULT_TIMEOUT_MILLIS = 3_000L;

    private final GatewayServer gatewayServer;
    private final ServiceNode serviceNode;
    private final SessionIdGenerator sessionIdGenerator;
    private final Map<Long, GatewayConnection> connectionsBySessionId = new ConcurrentHashMap<>();
    private final Map<Long, Set<GatewayConnection>> connectionsByUserId = new ConcurrentHashMap<>();
    private final Map<Long, QpsWindow> qpsWindows = new ConcurrentHashMap<>();
    private final Map<String, QpsWindow> ipQpsWindows = new ConcurrentHashMap<>();
    private final Map<Long, QpsWindow> userQpsWindows = new ConcurrentHashMap<>();
    private final Map<Integer, QpsWindow> actionQpsWindows = new ConcurrentHashMap<>();
    private final Map<String, QpsWindow> connectionAttemptWindows = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> connectionCountsByIp = new ConcurrentHashMap<>();
    private final Object connectionAdmissionLock = new Object();
    private final LongAdder outboundBackpressureCount = new LongAdder();
    private final LongAdder outboundDroppedCount = new LongAdder();
    private final LongAdder slowClientClosedCount = new LongAdder();
    private final LongAdder rejectedConnectionCount = new LongAdder();
    private final LongAdder rateLimitedRequestCount = new LongAdder();
    private final LongAdder kickedConnectionCount = new LongAdder();
    private final AtomicLong pendingOutboundBytes = new AtomicLong();
    private final AtomicLong pendingOutboundMessages = new AtomicLong();
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor();
    private int admittedConnectionCount;

    public GatewayRuntime(GatewayServer gatewayServer) {
        this.gatewayServer = gatewayServer;
        this.serviceNode = gatewayServer.getServiceNode();
        this.serviceNode.setGatewayKickHandler(this::handleGatewayKick);
        this.sessionIdGenerator = new SessionIdGenerator(
                SessionIdGenerator.resolveWorkerId(this.serviceNode.getInstanceId()));
    }

    public void start() {
        long idleTimeout = Math.max(1L, gatewayServer.getSessionIdleTimeoutSeconds());
        long interval = Math.min(30L, idleTimeout);
        sweeper.scheduleAtFixedRate(this::evictIdleConnections, interval, interval, TimeUnit.SECONDS);
    }

    public GatewayConnection registerConnection(GatewayConnection connection) {
        if (!tryRegisterConnection(connection)) {
            throw new IllegalStateException("Gateway connection rejected for remoteIp=" + normalizeIp(connection));
        }
        return connection;
    }

    public GatewayServer gatewayServer() {
        return gatewayServer;
    }

    public boolean tryRegisterConnection(GatewayConnection connection) {
        String remoteIp = normalizeIp(connection);
        String rejectionReason = tryAcquireConnectionSlot(remoteIp);
        if (rejectionReason != null) {
            rejectedConnectionCount.increment();
            log.warn("Gateway connection rejected protocol={} remote={} remoteIp={} reason={}",
                    connection.protocolType(), connection.remoteAddress(), remoteIp, rejectionReason);
            return false;
        }
        boolean registered = false;
        try {
            GatewayConnection oldConnection = connectionsBySessionId.put(
                    connection.userSession().getSessionId(), connection);
            if (oldConnection != null && oldConnection != connection) {
                releaseReplacedConnection(oldConnection);
                oldConnection.close();
            }
            addUserBinding(connection.userSession().getUserId(), connection);
            gatewayServer.getUserSessions().add(connection.userSession());
            UserHook hook = gatewayServer.getUserHook();
            if (hook != null) {
                hook.into(connection.userSession());
            }
            log.info("Gateway session connected sessionId={} protocol={} remote={}",
                    connection.userSession().getSessionId(), connection.protocolType(), connection.remoteAddress());
            registered = true;
            return true;
        } catch (RuntimeException e) {
            connectionsBySessionId.remove(connection.userSession().getSessionId(), connection);
            removeUserBinding(connection.userSession().getUserId(), connection);
            gatewayServer.getUserSessions().remove(connection.userSession().getSessionId());
            throw e;
        } finally {
            if (!registered) {
                releaseConnectionSlot(remoteIp);
            }
        }
    }

    public boolean unregisterConnection(GatewayConnection connection) {
        long sessionId = connection.userSession().getSessionId();
        if (!connectionsBySessionId.remove(sessionId, connection)) {
            return false;
        }
        releaseConnectionSlot(normalizeIp(connection));
        qpsWindows.remove(sessionId);
        removeUserBinding(connection.userSession().getUserId(), connection);
        UserSession removed = gatewayServer.getUserSessions().remove(sessionId);
        if (removed != null) {
            UserHook hook = gatewayServer.getUserHook();
            if (hook != null) {
                hook.quit(removed);
            }
            log.info("Gateway session disconnected sessionId={} protocol={} remote={}",
                    removed.getSessionId(), connection.protocolType(), connection.remoteAddress());
        }
        return true;
    }

    public boolean allowOutboundWrite(GatewayConnection connection) {
        GatewaySlowClientPolicy policy = gatewayServer.getSlowClientPolicy();
        if (policy == GatewaySlowClientPolicy.BUFFER) {
            return true;
        }
        recordOutboundBackpressure();
        if (policy == GatewaySlowClientPolicy.DROP) {
            recordOutboundDrop();
        }
        if (policy == GatewaySlowClientPolicy.CLOSE) {
            closeSlowConnection(connection, "channel is not writable", 0L, 0);
        }
        return false;
    }

    public void recordOutboundBackpressure() {
        outboundBackpressureCount.increment();
    }

    public void recordOutboundDrop() {
        outboundDroppedCount.increment();
    }

    public void outboundWriteQueued(long bytes) {
        pendingOutboundBytes.addAndGet(bytes);
        pendingOutboundMessages.incrementAndGet();
    }

    public void outboundWriteCompleted(long bytes) {
        pendingOutboundBytes.addAndGet(-bytes);
        pendingOutboundMessages.decrementAndGet();
    }

    public void closeSlowConnection(GatewayConnection connection, String reason,
                                    long pendingBytes, int pendingMessages) {
        if (!unregisterConnection(connection)) {
            connection.close();
            return;
        }
        slowClientClosedCount.increment();
        log.warn("Close slow Gateway connection sessionId={} remote={} remoteIp={} reason={} "
                        + "pendingBytes={} pendingMessages={}",
                connection.userSession().getSessionId(), connection.remoteAddress(), normalizeIp(connection),
                reason, pendingBytes, pendingMessages);
        connection.close();
    }

    public long outboundBackpressureCount() {
        return outboundBackpressureCount.sum();
    }

    public long outboundDroppedCount() {
        return outboundDroppedCount.sum();
    }

    public long slowClientClosedCount() {
        return slowClientClosedCount.sum();
    }

    public long kickedConnectionCount() {
        return kickedConnectionCount.sum();
    }

    public long pendingOutboundBytes() {
        return pendingOutboundBytes.get();
    }

    public long pendingOutboundMessages() {
        return pendingOutboundMessages.get();
    }

    public long rejectedConnectionCount() {
        return rejectedConnectionCount.sum();
    }

    public long rateLimitedRequestCount() {
        return rateLimitedRequestCount.sum();
    }

    public int onlineConnectionCount() {
        return connectionsBySessionId.size();
    }

    public void handleIncoming(GatewayConnection connection, GatewayPacket packet) {
        UserSession session = connection.userSession();
        session.touch();
        switch (packet.type()) {
            case PING -> connection.send(GatewayPacket.pong(
                    packet.requestId(),
                    session.getSessionId(),
                    session.getUserId(),
                    session.getAttachmentData()));
            case REQUEST -> handleRequest(connection, packet, session);
            default -> log.debug("Ignore inbound gateway packet type={} sessionId={}", packet.type(), session.getSessionId());
        }
    }

    public void handleBrokerPush(GatewayPushMessage pushMessage) {
        if (isBroadcastEnvelope(pushMessage)) {
            handlePlayerBroadcast(pushMessage);
            return;
        }
        List<GatewayConnection> targets = resolveTargets(pushMessage.getSessionId(), pushMessage.getUserId());
        if (targets.isEmpty()) {
            log.debug("Gateway push dropped because no local session matched sessionId={} userId={}",
                    pushMessage.getSessionId(), pushMessage.getUserId());
            return;
        }
        GatewayPacket packet = GatewayPacket.push(
                pushMessage.getCmd(),
                pushMessage.getAct(),
                pushMessage.getSessionId(),
                pushMessage.getUserId(),
                pushMessage.getPayload());
        for (GatewayConnection connection : targets) {
            connection.send(packet);
        }
    }

    private boolean isBroadcastEnvelope(GatewayPushMessage pushMessage) {
        return pushMessage.getSessionId() <= 0
                && pushMessage.getUserId() <= 0
                && pushMessage.getPayload() != null
                && pushMessage.getPayload().length > 0;
    }

    private void handlePlayerBroadcast(GatewayPushMessage pushMessage) {
        PlayerBroadcastEnvelope envelope = ProtobufPayloadCodec.decode(pushMessage.getPayload(), PlayerBroadcastEnvelope.class);
        if (envelope == null) {
            return;
        }
        List<GatewayConnection> targets = resolveTargets(envelope.getPlayerIds());
        if (targets.isEmpty() && envelope.getPlayerIds() != null && !envelope.getPlayerIds().isEmpty()) {
            return;
        }
        if ((envelope.getPlayerIds() == null || envelope.getPlayerIds().isEmpty())) {
            targets = allOpenConnections();
        }
        if (targets.isEmpty()) {
            return;
        }
        int cmd = ActionKey.cmdOf(envelope.getMsgId());
        int act = ActionKey.actOf(envelope.getMsgId());
        for (GatewayConnection connection : targets) {
            UserSession session = connection.userSession();
            connection.send(GatewayPacket.push(cmd, act, session.getSessionId(), session.getUserId(), envelope.getPayload()));
        }
    }

    /**
     * 服务端权威踢人入口：sessionId > 0 精确踢该连接；否则 userId > 0 踢该用户
     * 当前所有连接但跳过 exceptSessionId（顶号保留新会话）。走 unregisterConnection，
     * UserHook.quit 与连接配额释放照常执行，随后关闭传输。
     */
    public void handleGatewayKick(long sessionId, long userId, long exceptSessionId, String reason) {
        List<GatewayConnection> targets = new ArrayList<>();
        if (sessionId > 0) {
            GatewayConnection connection = connectionsBySessionId.get(sessionId);
            if (connection != null && connection.isOpen()) {
                targets.add(connection);
            }
        } else if (userId > 0) {
            Set<GatewayConnection> connections = connectionsByUserId.get(userId);
            if (connections != null) {
                for (GatewayConnection connection : connections) {
                    if (connection.isOpen() && connection.userSession().getSessionId() != exceptSessionId) {
                        targets.add(connection);
                    }
                }
            }
        } else {
            return;
        }
        for (GatewayConnection connection : targets) {
            boolean registered = unregisterConnection(connection);
            connection.close();
            if (registered) {
                kickedConnectionCount.increment();
                log.info("Gateway session kicked sessionId={} userId={} reason={}",
                        connection.userSession().getSessionId(),
                        connection.userSession().getUserId(),
                        reason == null || reason.isBlank() ? "unspecified" : reason);
            }
        }
    }

    @Override
    public void close() {
        sweeper.shutdownNow();
    }

    private void handleRequest(GatewayConnection connection, GatewayPacket packet, UserSession session) {
        log.debug("Gateway request: cmd={} act={} requestId={} sessionId={} userId={} oneWay={} payloadLen={}",
                packet.cmd(), packet.act(), packet.requestId(),
                session.getSessionId(), session.getUserId(),
                packet.oneWay(), packet.payload() != null ? packet.payload().length : 0);
        if (packet.cmd() <= 0 || packet.act() <= 0) {
            connection.send(GatewayPacket.error(packet.requestId(), packet.cmd(), packet.act(),
                    session.getSessionId(), session.getUserId(), "BAD_REQUEST", "cmd and act must be positive",
                    false, null));
            return;
        }

        int action = ActionKey.of(packet.cmd(), packet.act());
        String rateLimitReason = rateLimitReason(connection, session, action);
        if (rateLimitReason != null) {
            rateLimitedRequestCount.increment();
            connection.send(GatewayPacket.error(packet.requestId(), packet.cmd(), packet.act(),
                    session.getSessionId(), session.getUserId(), "RATE_LIMITED", rateLimitReason,
                    false, null));
            return;
        }

        if (gatewayServer.getAccessControl().isDenied(packet.cmd())) {
            connection.send(GatewayPacket.error(packet.requestId(), packet.cmd(), packet.act(),
                    session.getSessionId(), session.getUserId(), "ACTION_DENIED", "action cmd is denied",
                    false, null));
            return;
        }

        GatewayAuthenticationResult authentication = authenticate(connection, session, packet);
        if (authentication != null && !authentication.allowed()) {
            connection.send(GatewayPacket.error(packet.requestId(), packet.cmd(), packet.act(),
                    session.getSessionId(), session.getUserId(),
                    authentication.errorCode(), authentication.errorMessage(),
                    false, null));
            return;
        }

        if (gatewayServer.getAccessControl().requiresAuth(action) && !session.isAuthenticated()) {
            connection.send(GatewayPacket.error(packet.requestId(), packet.cmd(), packet.act(),
                    session.getSessionId(), session.getUserId(), "UNAUTHORIZED", "authentication required",
                    false, null));
            return;
        }

        byte[] payload = packet.payload();
        Duration timeout = Duration.ofMillis(resolveTimeout(packet.timeoutMillis()));
        InvokeOptions options = InvokeOptions.builder()
                .gatewayInstanceId(session.getGatewayInstanceId())
                .sessionId(session.getSessionId())
                .userId(session.getUserId())
                .attachmentData(session.getAttachmentData())
                .timeout(timeout)
                .build();
        if (packet.oneWay()) {
            try {
                serviceNode.invokeOneWay(packet.cmd(), packet.act(), payload, options);
            } catch (RuntimeException e) {
                log.warn("Gateway broker one-way send failed: cmd={} act={} requestId={} error={}",
                        packet.cmd(), packet.act(), packet.requestId(), e.getMessage());
            }
            return;
        }

        serviceNode.invokeAsync(packet.cmd(), packet.act(), payload, options)
                .whenComplete((result, throwable) -> {
                    if (!connection.isOpen()) {
                        log.debug("Gateway response dropped: connection closed requestId={}", packet.requestId());
                        return;
                    }
                    if (throwable != null) {
                        log.warn("Gateway broker error: cmd={} act={} requestId={} error={}",
                                packet.cmd(), packet.act(), packet.requestId(), throwable.getMessage());
                        connection.send(GatewayPacket.error(packet.requestId(), packet.cmd(), packet.act(),
                                session.getSessionId(), session.getUserId(),
                                "BROKER_ERROR", throwable.getMessage(),
                                false, null));
                        return;
                    }
                    log.debug("Gateway broker response: cmd={} act={} requestId={} success={} errorCode={} dataLen={}",
                            packet.cmd(), packet.act(), packet.requestId(),
                            result.isSuccess(), result.getErrorCode(),
                            result.isSuccess() && result.getData() != null
                                    ? ProtobufPayloadCodec.encode(result.getData()).length : 0);
                    boolean attachmentUpdated = result.isAttachmentUpdated();
                    if (attachmentUpdated) {
                        session.setAttachmentData(result.getAttachmentData());
                        syncSessionAuthState(connection, session);
                    }
                    if (result.isSuccess()) {
                        connection.send(GatewayPacket.response(packet.requestId(), packet.cmd(), packet.act(),
                                session.getSessionId(), session.getUserId(),
                                result.isAttachmentUpdated(),
                                result.isAttachmentUpdated() ? session.getAttachmentData() : null,
                                encodeResult(result)));
                    } else {
                        connection.send(GatewayPacket.error(packet.requestId(), packet.cmd(), packet.act(),
                                session.getSessionId(), session.getUserId(),
                                result.getErrorCode(), result.getErrorMessage(),
                                result.isAttachmentUpdated(),
                                result.isAttachmentUpdated() ? session.getAttachmentData() : null));
                    }
                });
    }

    private byte[] encodeResult(ActionResult result) {
        return ProtobufPayloadCodec.encode(result.getData());
    }

    private long resolveTimeout(long timeoutMillis) {
        return timeoutMillis > 0 ? timeoutMillis : DEFAULT_TIMEOUT_MILLIS;
    }

    private String rateLimitReason(GatewayConnection connection, UserSession session, int action) {
        if (exceeds(qpsWindows, session.getSessionId(), gatewayServer.getMaxQpsPerPlayer())) {
            return "session qps exceeded";
        }
        if (exceeds(ipQpsWindows, normalizeIp(connection), gatewayServer.getMaxQpsPerIp())) {
            return "ip qps exceeded";
        }
        if (session.getUserId() > 0
                && exceeds(userQpsWindows, session.getUserId(), gatewayServer.getMaxQpsPerUser())) {
            return "user qps exceeded";
        }
        if (exceeds(actionQpsWindows, action, gatewayServer.getMaxQpsPerAction())) {
            return "action qps exceeded";
        }
        return null;
    }

    private List<GatewayConnection> resolveTargets(long sessionId, long userId) {
        List<GatewayConnection> targets = new ArrayList<>();
        if (sessionId > 0) {
            GatewayConnection connection = connectionsBySessionId.get(sessionId);
            if (connection != null && connection.isOpen()) {
                targets.add(connection);
            }
            return targets;
        }
        if (userId <= 0) {
            return targets;
        }
        Set<GatewayConnection> connections = connectionsByUserId.get(userId);
        if (connections == null || connections.isEmpty()) {
            return targets;
        }
        for (GatewayConnection connection : connections) {
            if (connection.isOpen()) {
                targets.add(connection);
            }
        }
        return targets;
    }

    private List<GatewayConnection> resolveTargets(List<Long> playerIds) {
        List<GatewayConnection> targets = new ArrayList<>();
        if (playerIds == null || playerIds.isEmpty()) {
            return targets;
        }
        Set<GatewayConnection> uniqueTargets = ConcurrentHashMap.newKeySet();
        for (Long playerId : playerIds) {
            if (playerId == null || playerId <= 0) {
                continue;
            }
            Set<GatewayConnection> connections = connectionsByUserId.get(playerId);
            if (connections == null || connections.isEmpty()) {
                continue;
            }
            for (GatewayConnection connection : connections) {
                if (connection.isOpen() && uniqueTargets.add(connection)) {
                    targets.add(connection);
                }
            }
        }
        return targets;
    }

    private List<GatewayConnection> allOpenConnections() {
        List<GatewayConnection> targets = new ArrayList<>();
        for (GatewayConnection connection : connectionsBySessionId.values()) {
            if (connection.isOpen()) {
                targets.add(connection);
            }
        }
        return targets;
    }

    private void evictIdleConnections() {
        Instant expireBefore = Instant.now().minusSeconds(Math.max(1L, gatewayServer.getSessionIdleTimeoutSeconds()));
        for (GatewayConnection connection : connectionsBySessionId.values()) {
            if (connection.userSession().getLastActiveTime().isAfter(expireBefore)) {
                continue;
            }
            if (unregisterConnection(connection)) {
                connection.close();
            }
        }
        evictStaleRateWindows();
    }

    public UserSession newSession(String protocolType, String remoteAddress) {
        return new UserSession(sessionIdGenerator.nextId(), protocolType, serviceNode.getInstanceId(), remoteAddress);
    }

    private void syncSessionAuthState(GatewayConnection connection, UserSession session) {
        long resolvedUserId = resolveUserIdFromAttachment(session);
        session.setAuthenticated(resolvedUserId > 0);
        long currentUserId = session.getUserId();
        if (currentUserId == resolvedUserId) {
            return;
        }
        removeUserBinding(currentUserId, connection);
        session.setUserId(resolvedUserId);
        addUserBinding(resolvedUserId, connection);
        UserHook hook = gatewayServer.getUserHook();
        if (hook != null) {
            hook.bind(session, currentUserId);
        }
    }

    private long resolveUserIdFromAttachment(UserSession session) {
        byte[] attachmentData = session.getAttachmentData();
        if (attachmentData == null || attachmentData.length == 0) {
            return 0L;
        }
        BaseAttachment attachment = ProtobufPayloadCodec.decode(attachmentData, BaseAttachment.class);
        return attachment != null ? attachment.getUserId() : 0L;
    }

    private void addUserBinding(long userId, GatewayConnection connection) {
        if (userId <= 0) {
            return;
        }
        connectionsByUserId.computeIfAbsent(userId, key -> ConcurrentHashMap.newKeySet()).add(connection);
    }

    private void removeUserBinding(long userId, GatewayConnection connection) {
        if (userId <= 0) {
            return;
        }
        Set<GatewayConnection> connections = connectionsByUserId.get(userId);
        if (connections == null) {
            return;
        }
        connections.remove(connection);
        if (connections.isEmpty()) {
            connectionsByUserId.remove(userId, connections);
            userQpsWindows.remove(userId);
        }
    }

    private GatewayAuthenticationResult authenticate(GatewayConnection connection, UserSession session,
                                                     GatewayPacket packet) {
        GatewayRequestAuthenticator authenticator = gatewayServer.getRequestAuthenticator();
        if (authenticator == null) {
            return GatewayAuthenticationResult.allow();
        }
        try {
            GatewayAuthenticationResult result = authenticator.authenticate(new GatewayRequestContext(
                    session,
                    packet,
                    connection.protocolType(),
                    connection.remoteAddress(),
                    normalizeIp(connection)));
            return result == null ? GatewayAuthenticationResult.allow() : result;
        } catch (RuntimeException e) {
            log.warn("Gateway request authenticator failed sessionId={} remote={} error={}",
                    session.getSessionId(), connection.remoteAddress(), e.getMessage());
            return GatewayAuthenticationResult.deny("AUTHENTICATION_ERROR", "gateway authentication failed");
        }
    }

    private String tryAcquireConnectionSlot(String remoteIp) {
        synchronized (connectionAdmissionLock) {
            int attemptsLimit = gatewayServer.getMaxConnectionAttemptsPerIpPerSecond();
            if (attemptsLimit > 0 && connectionAttemptWindows
                    .computeIfAbsent(remoteIp, ignored -> new QpsWindow())
                    .incrementAndCheck(attemptsLimit)) {
                return "ip connection rate exceeded";
            }
            int maxConnections = gatewayServer.getMaxConnections();
            if (maxConnections > 0 && admittedConnectionCount >= maxConnections) {
                return "gateway connection capacity reached";
            }
            AtomicInteger ipConnections = connectionCountsByIp.computeIfAbsent(
                    remoteIp, ignored -> new AtomicInteger());
            int maxPerIp = gatewayServer.getMaxConnectionsPerIp();
            if (maxPerIp > 0 && ipConnections.get() >= maxPerIp) {
                return "ip connection capacity reached";
            }
            admittedConnectionCount++;
            ipConnections.incrementAndGet();
            return null;
        }
    }

    private void releaseConnectionSlot(String remoteIp) {
        synchronized (connectionAdmissionLock) {
            if (admittedConnectionCount > 0) {
                admittedConnectionCount--;
            }
            AtomicInteger count = connectionCountsByIp.get(remoteIp);
            if (count != null && count.decrementAndGet() <= 0) {
                connectionCountsByIp.remove(remoteIp, count);
                ipQpsWindows.remove(remoteIp);
            }
        }
    }

    private void releaseReplacedConnection(GatewayConnection connection) {
        long sessionId = connection.userSession().getSessionId();
        releaseConnectionSlot(normalizeIp(connection));
        qpsWindows.remove(sessionId);
        removeUserBinding(connection.userSession().getUserId(), connection);
        UserSession removed = gatewayServer.getUserSessions().remove(sessionId);
        if (removed != null && gatewayServer.getUserHook() != null) {
            gatewayServer.getUserHook().quit(removed);
        }
    }

    private void evictStaleRateWindows() {
        long expireBeforeMillis = System.currentTimeMillis()
                - TimeUnit.SECONDS.toMillis(Math.max(60L, gatewayServer.getSessionIdleTimeoutSeconds()));
        evictStale(ipQpsWindows, expireBeforeMillis);
        evictStale(userQpsWindows, expireBeforeMillis);
        evictStale(actionQpsWindows, expireBeforeMillis);
        evictStale(connectionAttemptWindows, expireBeforeMillis);
    }

    private <K> void evictStale(Map<K, QpsWindow> windows, long expireBeforeMillis) {
        windows.entrySet().removeIf(entry -> entry.getValue().lastAccessMillis() < expireBeforeMillis);
    }

    private static <K> boolean exceeds(Map<K, QpsWindow> windows, K key, long limit) {
        return limit > 0 && windows.computeIfAbsent(key, ignored -> new QpsWindow()).incrementAndCheck(limit);
    }

    private static String normalizeIp(GatewayConnection connection) {
        String remoteIp = connection.remoteIp();
        if (remoteIp == null || remoteIp.isBlank()) {
            return "unknown";
        }
        return remoteIp.trim().toLowerCase(java.util.Locale.ROOT);
    }

    private static final class QpsWindow {
        private long windowStartMillis;
        private long count;
        private volatile long lastAccessMillis = System.currentTimeMillis();

        private synchronized boolean incrementAndCheck(long maxQps) {
            long nowMillis = System.currentTimeMillis();
            lastAccessMillis = nowMillis;
            if (windowStartMillis == 0L || nowMillis - windowStartMillis >= 1_000L) {
                windowStartMillis = nowMillis;
                count = 0;
            }
            count++;
            return count > maxQps;
        }

        private long lastAccessMillis() {
            return lastAccessMillis;
        }
    }
}
