package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.ServiceNodeAware;
import cn.itsub.kynet.framework.user.UserHook;
import cn.itsub.kynet.framework.user.UserSessions;
import cn.itsub.kynet.framework.user.UserSessionsAware;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

public class GatewayServer {
    public static final long DEFAULT_MAX_QPS_PER_PLAYER = 1_000L;
    public static final long DEFAULT_MAX_QPS_PER_IP = 20_000L;
    public static final long DEFAULT_MAX_QPS_PER_USER = 2_000L;
    public static final long DEFAULT_MAX_QPS_PER_ACTION = 50_000L;
    public static final long DEFAULT_SESSION_IDLE_TIMEOUT_SECONDS = 300L;
    public static final int DEFAULT_MAX_CONNECTIONS = 100_000;
    public static final int DEFAULT_MAX_CONNECTIONS_PER_IP = 1_000;
    public static final int DEFAULT_MAX_CONNECTION_ATTEMPTS_PER_IP_PER_SECOND = 200;
    public static final int DEFAULT_MAX_FRAME_LENGTH = 1024 * 1024;
    public static final int DEFAULT_MAX_PAYLOAD_LENGTH = 512 * 1024;
    public static final int DEFAULT_MAX_STRING_LENGTH = 8 * 1024;
    public static final int DEFAULT_WRITE_BUFFER_LOW_WATER_MARK = 128 * 1024;
    public static final int DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK = 512 * 1024;
    public static final long DEFAULT_MAX_PENDING_WRITE_BYTES = 4L * 1024 * 1024;
    public static final int DEFAULT_MAX_PENDING_WRITE_MESSAGES = 2_000;
    public static final long DEFAULT_MAX_UNWRITABLE_DURATION_MILLIS = 5_000L;
    public static final GatewaySlowClientPolicy DEFAULT_SLOW_CLIENT_POLICY = GatewaySlowClientPolicy.BUFFER;

    private final ServiceNode serviceNode;
    private final String[] endpoints;
    private final AccessControl accessControl = new AccessControl();
    private final UserSessions userSessions = new UserSessions();
    private UserHook userHook;
    private GatewayPushHandler gatewayPushHandler;
    private GatewayRequestAuthenticator requestAuthenticator;
    private GatewayTlsConfig tlsConfig;
    private long maxQpsPerPlayer = DEFAULT_MAX_QPS_PER_PLAYER;
    private long maxQpsPerIp = DEFAULT_MAX_QPS_PER_IP;
    private long maxQpsPerUser = DEFAULT_MAX_QPS_PER_USER;
    private long maxQpsPerAction = DEFAULT_MAX_QPS_PER_ACTION;
    private long sessionIdleTimeoutSeconds = DEFAULT_SESSION_IDLE_TIMEOUT_SECONDS;
    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private int maxConnectionsPerIp = DEFAULT_MAX_CONNECTIONS_PER_IP;
    private int maxConnectionAttemptsPerIpPerSecond = DEFAULT_MAX_CONNECTION_ATTEMPTS_PER_IP_PER_SECOND;
    private int maxFrameLength = DEFAULT_MAX_FRAME_LENGTH;
    private int maxPayloadLength = DEFAULT_MAX_PAYLOAD_LENGTH;
    private int maxStringLength = DEFAULT_MAX_STRING_LENGTH;
    private int writeBufferLowWaterMark = DEFAULT_WRITE_BUFFER_LOW_WATER_MARK;
    private int writeBufferHighWaterMark = DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK;
    private long maxPendingWriteBytes = DEFAULT_MAX_PENDING_WRITE_BYTES;
    private int maxPendingWriteMessages = DEFAULT_MAX_PENDING_WRITE_MESSAGES;
    private long maxUnwritableDurationMillis = DEFAULT_MAX_UNWRITABLE_DURATION_MILLIS;
    private GatewaySlowClientPolicy slowClientPolicy = DEFAULT_SLOW_CLIENT_POLICY;
    private volatile Set<String> allowedWebSocketOrigins = Set.of();

    public GatewayServer(ServiceNode serviceNode, String... endpoints) {
        this.serviceNode = serviceNode;
        this.endpoints = endpoints;
    }

    public AccessControl getAccessControl() {
        return accessControl;
    }

    public void setMaxQpsPerPlayer(long maxQpsPerPlayer) {
        this.maxQpsPerPlayer = nonNegative(maxQpsPerPlayer, "maxQpsPerPlayer");
    }

    public long getMaxQpsPerPlayer() {
        return maxQpsPerPlayer;
    }

    public void setMaxQpsPerIp(long maxQpsPerIp) {
        this.maxQpsPerIp = nonNegative(maxQpsPerIp, "maxQpsPerIp");
    }

    public long getMaxQpsPerIp() {
        return maxQpsPerIp;
    }

    public void setMaxQpsPerUser(long maxQpsPerUser) {
        this.maxQpsPerUser = nonNegative(maxQpsPerUser, "maxQpsPerUser");
    }

    public long getMaxQpsPerUser() {
        return maxQpsPerUser;
    }

    public void setMaxQpsPerAction(long maxQpsPerAction) {
        this.maxQpsPerAction = nonNegative(maxQpsPerAction, "maxQpsPerAction");
    }

    public long getMaxQpsPerAction() {
        return maxQpsPerAction;
    }

    public long getSessionIdleTimeoutSeconds() {
        return sessionIdleTimeoutSeconds;
    }

    public void setSessionIdleTimeoutSeconds(long sessionIdleTimeoutSeconds) {
        if (sessionIdleTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("sessionIdleTimeoutSeconds must be positive");
        }
        this.sessionIdleTimeoutSeconds = sessionIdleTimeoutSeconds;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = nonNegative(maxConnections, "maxConnections");
    }

    public int getMaxConnectionsPerIp() {
        return maxConnectionsPerIp;
    }

    public void setMaxConnectionsPerIp(int maxConnectionsPerIp) {
        this.maxConnectionsPerIp = nonNegative(maxConnectionsPerIp, "maxConnectionsPerIp");
    }

    public int getMaxConnectionAttemptsPerIpPerSecond() {
        return maxConnectionAttemptsPerIpPerSecond;
    }

    public void setMaxConnectionAttemptsPerIpPerSecond(int maxConnectionAttemptsPerIpPerSecond) {
        this.maxConnectionAttemptsPerIpPerSecond = nonNegative(
                maxConnectionAttemptsPerIpPerSecond, "maxConnectionAttemptsPerIpPerSecond");
    }

    public int getMaxFrameLength() {
        return maxFrameLength;
    }

    public void setMaxFrameLength(int maxFrameLength) {
        if (maxFrameLength < 64) {
            throw new IllegalArgumentException("maxFrameLength must be at least 64 bytes");
        }
        this.maxFrameLength = maxFrameLength;
        if (maxPayloadLength > maxFrameLength) {
            maxPayloadLength = maxFrameLength;
        }
        if (maxStringLength > maxFrameLength) {
            maxStringLength = maxFrameLength;
        }
    }

    public int getMaxPayloadLength() {
        return maxPayloadLength;
    }

    public void setMaxPayloadLength(int maxPayloadLength) {
        if (maxPayloadLength < 0 || maxPayloadLength > maxFrameLength) {
            throw new IllegalArgumentException("maxPayloadLength must be between 0 and maxFrameLength");
        }
        this.maxPayloadLength = maxPayloadLength;
    }

    public int getMaxStringLength() {
        return maxStringLength;
    }

    public void setMaxStringLength(int maxStringLength) {
        if (maxStringLength < 0 || maxStringLength > maxFrameLength) {
            throw new IllegalArgumentException("maxStringLength must be between 0 and maxFrameLength");
        }
        this.maxStringLength = maxStringLength;
    }

    public int getWriteBufferLowWaterMark() {
        return writeBufferLowWaterMark;
    }

    public int getWriteBufferHighWaterMark() {
        return writeBufferHighWaterMark;
    }

    public void setWriteBufferWaterMark(int lowWaterMark, int highWaterMark) {
        if (lowWaterMark < 0 || highWaterMark <= lowWaterMark) {
            throw new IllegalArgumentException("write buffer water mark requires 0 <= low < high");
        }
        this.writeBufferLowWaterMark = lowWaterMark;
        this.writeBufferHighWaterMark = highWaterMark;
    }

    public long getMaxPendingWriteBytes() {
        return maxPendingWriteBytes;
    }

    public void setMaxPendingWriteBytes(long maxPendingWriteBytes) {
        this.maxPendingWriteBytes = nonNegative(maxPendingWriteBytes, "maxPendingWriteBytes");
    }

    public int getMaxPendingWriteMessages() {
        return maxPendingWriteMessages;
    }

    public void setMaxPendingWriteMessages(int maxPendingWriteMessages) {
        this.maxPendingWriteMessages = nonNegative(maxPendingWriteMessages, "maxPendingWriteMessages");
    }

    public long getMaxUnwritableDurationMillis() {
        return maxUnwritableDurationMillis;
    }

    public void setMaxUnwritableDurationMillis(long maxUnwritableDurationMillis) {
        this.maxUnwritableDurationMillis = nonNegative(
                maxUnwritableDurationMillis, "maxUnwritableDurationMillis");
    }

    public void setMaxUnwritableDurationSeconds(long maxUnwritableDurationSeconds) {
        setMaxUnwritableDurationMillis(Math.multiplyExact(
                nonNegative(maxUnwritableDurationSeconds, "maxUnwritableDurationSeconds"), 1_000L));
    }

    public GatewaySlowClientPolicy getSlowClientPolicy() {
        return slowClientPolicy;
    }

    public void setSlowClientPolicy(GatewaySlowClientPolicy slowClientPolicy) {
        if (slowClientPolicy == null) {
            throw new IllegalArgumentException("slowClientPolicy must not be null");
        }
        this.slowClientPolicy = slowClientPolicy;
    }

    public GatewayTlsConfig getTlsConfig() {
        return tlsConfig;
    }

    public void setTlsConfig(GatewayTlsConfig tlsConfig) {
        this.tlsConfig = tlsConfig;
    }

    public GatewayRequestAuthenticator getRequestAuthenticator() {
        return requestAuthenticator;
    }

    public void setRequestAuthenticator(GatewayRequestAuthenticator requestAuthenticator) {
        this.requestAuthenticator = requestAuthenticator;
    }

    public void setAllowedWebSocketOrigins(String... origins) {
        if (origins == null || origins.length == 0) {
            allowedWebSocketOrigins = Set.of();
            return;
        }
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        Arrays.stream(origins)
                .filter(origin -> origin != null && !origin.isBlank())
                .map(GatewayServer::normalizeOrigin)
                .forEach(normalized::add);
        allowedWebSocketOrigins = Set.copyOf(normalized);
    }

    public Set<String> getAllowedWebSocketOrigins() {
        return allowedWebSocketOrigins;
    }

    public boolean isWebSocketOriginAllowed(String origin) {
        Set<String> allowedOrigins = allowedWebSocketOrigins;
        if (allowedOrigins.isEmpty() || allowedOrigins.contains("*")) {
            return true;
        }
        return origin != null && !origin.isBlank() && allowedOrigins.contains(normalizeOrigin(origin));
    }

    public String[] getEndpoints() {
        return endpoints;
    }

    public ServiceNode getServiceNode() {
        return serviceNode;
    }

    public UserSessions getUserSessions() {
        return userSessions;
    }

    public void setUserHook(UserHook userHook) {
        this.userHook = userHook;
        if (userHook instanceof UserSessionsAware aware) {
            aware.setUserSessions(userSessions);
        }
        if (userHook instanceof ServiceNodeAware aware) {
            aware.setServiceNode(serviceNode);
        }
    }

    public UserHook getUserHook() {
        return userHook;
    }

    public GatewayPushHandler getGatewayPushHandler() {
        return gatewayPushHandler;
    }

    public void setGatewayPushHandler(GatewayPushHandler gatewayPushHandler) {
        this.gatewayPushHandler = gatewayPushHandler;
    }

    private static int nonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static long nonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return value;
    }

    private static String normalizeOrigin(String origin) {
        return origin.trim().toLowerCase(Locale.ROOT);
    }
}
