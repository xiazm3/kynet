package cn.itsub.kynet.registry;

import cn.itsub.kynet.common.transport.AeronTransport;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

final class RegistryNodeConfig {
    static final String DEFAULT_CLUSTER_NAME = "kynet";

    private final boolean clusterEnabled;
    private final String clusterName;
    private final String nodeId;
    private final long epoch;
    private final String clientBindHost;
    private final String clientHost;
    private final int clientPort;
    private final String clusterBindHost;
    private final String clusterHost;
    private final int clusterPort;
    private final int discoveryPort;
    private final String discoveryBindHost;
    private final String discoveryAddress;
    private final long discoveryIntervalMillis;
    private final long gossipIntervalMillis;
    private final long suspectTimeoutMillis;
    private final long deadTimeoutMillis;
    private final byte[] clusterSecret;
    private final ThreadLocal<Mac> protocolMac;
    private final long protocolClockSkewMillis;
    private final int gossipRoutesPerChunk;
    private final long routeSnapshotIntervalMillis;
    private final int clusterSocketBufferBytes;
    private final AtomicLong protocolNonce = new AtomicLong(
            ThreadLocalRandom.current().nextLong(1L, Long.MAX_VALUE / 2));

    private RegistryNodeConfig(boolean clusterEnabled, String clusterName, String nodeId, long epoch,
                             String clientBindHost, String clientHost, int clientPort,
                             String clusterBindHost, String clusterHost,
                             int clusterPort, int discoveryPort, String discoveryBindHost,
                             String discoveryAddress,
                             long discoveryIntervalMillis, long gossipIntervalMillis,
                             long suspectTimeoutMillis, long deadTimeoutMillis,
                             byte[] clusterSecret, long protocolClockSkewMillis,
                             int gossipRoutesPerChunk, long routeSnapshotIntervalMillis,
                             int clusterSocketBufferBytes) {
        this.clusterEnabled = clusterEnabled;
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.epoch = epoch;
        this.clientBindHost = clientBindHost;
        this.clientHost = clientHost;
        this.clientPort = clientPort;
        this.clusterBindHost = clusterBindHost;
        this.clusterHost = clusterHost;
        this.clusterPort = clusterPort;
        this.discoveryPort = discoveryPort;
        this.discoveryBindHost = discoveryBindHost;
        this.discoveryAddress = discoveryAddress;
        this.discoveryIntervalMillis = discoveryIntervalMillis;
        this.gossipIntervalMillis = gossipIntervalMillis;
        this.suspectTimeoutMillis = suspectTimeoutMillis;
        this.deadTimeoutMillis = deadTimeoutMillis;
        this.clusterSecret = Arrays.copyOf(clusterSecret, clusterSecret.length);
        this.protocolMac = ThreadLocal.withInitial(this::newProtocolMac);
        this.protocolClockSkewMillis = protocolClockSkewMillis;
        this.gossipRoutesPerChunk = gossipRoutesPerChunk;
        this.routeSnapshotIntervalMillis = routeSnapshotIntervalMillis;
        this.clusterSocketBufferBytes = clusterSocketBufferBytes;
    }

    static RegistryNodeConfig fromSystemProperties(int clientPort) {
        boolean enabled = Boolean.parseBoolean(stringProperty("kynet.registry.cluster.enabled", "false"));
        String clusterName = stringProperty("kynet.registry.cluster.name", DEFAULT_CLUSTER_NAME);
        String defaultHost = AeronTransport.localHostAddressForReply();
        String clientBindHost = stringProperty("kynet.registry.clientBindHost", "0.0.0.0");
        String clientHost = stringProperty("kynet.registry.clientHost", defaultHost);
        String clusterHost = stringProperty("kynet.registry.clusterHost", defaultHost);
        String clusterBindHost = stringProperty("kynet.registry.clusterBindHost", "0.0.0.0");
        int clusterPort = positiveIntProperty("kynet.registry.clusterPort", AeronTransport.findAvailablePort());
        int discoveryPort = positiveIntProperty("kynet.registry.discoveryPort", 18899);
        String discoveryBindHost = stringProperty("kynet.registry.discoveryBindHost", "0.0.0.0");
        String discoveryAddress = stringProperty("kynet.registry.discoveryAddress", defaultBroadcastAddress());
        String nodeId = stringProperty("kynet.registry.nodeId",
                "registry-" + clientPort);
        long epoch = positiveLongProperty("kynet.registry.epoch", System.currentTimeMillis());
        byte[] clusterSecret = stringProperty("kynet.registry.cluster.secret", "")
                .getBytes(StandardCharsets.UTF_8);
        if (enabled && clusterSecret.length < 16) {
            throw new IllegalStateException(
                    "kynet.registry.cluster.secret must contain at least 16 UTF-8 bytes when clustering is enabled");
        }
        RegistryNodeConfig config = new RegistryNodeConfig(
                enabled,
                clusterName,
                nodeId,
                epoch,
                clientBindHost,
                clientHost,
                clientPort,
                clusterBindHost,
                clusterHost,
                clusterPort,
                discoveryPort,
                discoveryBindHost,
                discoveryAddress,
                positiveLongProperty("kynet.registry.discoveryIntervalMs", 1_000L),
                positiveLongProperty("kynet.registry.gossipIntervalMs", 500L),
                positiveLongProperty("kynet.registry.suspectTimeoutMs", 3_000L),
                positiveLongProperty("kynet.registry.deadTimeoutMs", 6_000L),
                clusterSecret,
                positiveLongProperty("kynet.registry.cluster.protocolClockSkewMs", 30_000L),
                positiveIntProperty("kynet.registry.cluster.gossipRoutesPerChunk", 64),
                positiveLongProperty("kynet.registry.cluster.routeSnapshotIntervalMs", 10_000L),
                positiveIntProperty("kynet.registry.cluster.socketBufferBytes", 16 * 1024 * 1024));
        config.validate();
        return config;
    }

    private static String stringProperty(String name, String defaultValue) {
        String value = configuredProperty(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String configuredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null && name.startsWith("kynet.registry.")) {
            value = System.getProperty("kynet.broker." + name.substring("kynet.registry.".length()));
        }
        return value;
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        String configured = configuredProperty(name);
        if (configured == null || configured.isBlank()) {
            return defaultValue;
        }
        try {
            int value = Integer.parseInt(configured);
            if (value <= 0) {
                throw new IllegalStateException(name + " must be positive");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be a valid integer", e);
        }
    }

    private static long positiveLongProperty(String name, long defaultValue) {
        String configured = configuredProperty(name);
        if (configured == null || configured.isBlank()) {
            return defaultValue;
        }
        try {
            long value = Long.parseLong(configured);
            if (value <= 0L) {
                throw new IllegalStateException(name + " must be positive");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be a valid long", e);
        }
    }

    private void validate() {
        validatePort("clientPort", clientPort);
        validatePort("kynet.registry.clusterPort", clusterPort);
        validatePort("kynet.registry.discoveryPort", discoveryPort);
        if (suspectTimeoutMillis >= deadTimeoutMillis) {
            throw new IllegalStateException(
                    "kynet.registry.suspectTimeoutMs must be less than kynet.registry.deadTimeoutMs");
        }
        if (gossipRoutesPerChunk > ClusterMessageCodec.MAX_ROUTES_PER_CHUNK) {
            throw new IllegalStateException(
                    "kynet.registry.cluster.gossipRoutesPerChunk must not exceed "
                            + ClusterMessageCodec.MAX_ROUTES_PER_CHUNK);
        }
        if (clusterEnabled && (isWildcardHost(clientHost) || isWildcardHost(clusterHost))) {
            throw new IllegalStateException(
                    "kynet.registry.clientHost and kynet.registry.clusterHost must be routable advertised addresses");
        }
    }

    private static void validatePort(String name, int port) {
        if (port <= 0 || port > 65_535) {
            throw new IllegalStateException(name + " must be between 1 and 65535");
        }
    }

    private static boolean isWildcardHost(String host) {
        return "0.0.0.0".equals(host) || "::".equals(host) || "[::]".equals(host);
    }

    private static String defaultBroadcastAddress() {
        try {
            InetAddress local = InetAddress.getLocalHost();
            byte[] address = local.getAddress();
            if (address.length == 4 && address[0] == 127) {
                return "255.255.255.255";
            }
        } catch (UnknownHostException ignored) {
        }
        return "255.255.255.255";
    }

    boolean clusterEnabled() {
        return clusterEnabled;
    }

    String clusterName() {
        return clusterName;
    }

    String nodeId() {
        return nodeId;
    }

    long epoch() {
        return epoch;
    }

    String clientHost() {
        return clientHost;
    }

    String clientBindHost() {
        return clientBindHost;
    }

    int clientPort() {
        return clientPort;
    }

    String clusterBindHost() {
        return clusterBindHost;
    }

    String clusterHost() {
        return clusterHost;
    }

    int clusterPort() {
        return clusterPort;
    }

    int discoveryPort() {
        return discoveryPort;
    }

    String discoveryBindHost() {
        return discoveryBindHost;
    }

    String discoveryAddress() {
        return discoveryAddress;
    }

    long discoveryIntervalMillis() {
        return discoveryIntervalMillis;
    }

    long gossipIntervalMillis() {
        return gossipIntervalMillis;
    }

    long suspectTimeoutMillis() {
        return suspectTimeoutMillis;
    }

    long deadTimeoutMillis() {
        return deadTimeoutMillis;
    }

    byte[] computeProtocolMac(byte[] payload, int length) {
        Mac mac = protocolMac.get();
        mac.reset();
        mac.update(payload, 0, length);
        return mac.doFinal();
    }

    private Mac newProtocolMac() {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(clusterSecret, "HmacSHA256"));
            return mac;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is unavailable", e);
        }
    }

    long protocolClockSkewMillis() {
        return protocolClockSkewMillis;
    }

    int gossipRoutesPerChunk() {
        return gossipRoutesPerChunk;
    }

    long routeSnapshotIntervalMillis() {
        return routeSnapshotIntervalMillis;
    }

    int clusterSocketBufferBytes() {
        return clusterSocketBufferBytes;
    }

    long nextProtocolNonce() {
        return protocolNonce.incrementAndGet();
    }


}
