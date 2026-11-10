package cn.itsub.kynet.registry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterMessageCodecTest {
    @AfterEach
    void clearProperties() {
        System.clearProperty("kynet.broker.cluster.enabled");
        System.clearProperty("kynet.broker.cluster.secret");
        System.clearProperty("kynet.broker.nodeId");
        System.clearProperty("kynet.broker.clusterHost");
        System.clearProperty("kynet.broker.clientHost");
        System.clearProperty("kynet.broker.clusterPort");
        System.clearProperty("kynet.broker.cluster.protocolClockSkewMs");
    }

    @Test
    void authenticatedMessageRejectsTamperingAndWrongSecret() {
        RegistryNodeConfig sender = config("codec-a", "codec-shared-secret", 19301, 19401);
        byte[] encoded = ClusterMessageCodec.encodeDiscovery(
                sender, new RegistryMember("codec-a", sender.epoch(), "127.0.0.1",
                        19301, 19401, 1L, MemberState.ALIVE, 1L, System.currentTimeMillis()));

        assertInstanceOf(ClusterMessageCodec.DiscoveryMessage.class,
                ClusterMessageCodec.decode(sender, encoded, encoded.length));

        byte[] tampered = Arrays.copyOf(encoded, encoded.length);
        tampered[tampered.length / 2] ^= 1;
        assertNull(ClusterMessageCodec.decode(sender, tampered, tampered.length));

        RegistryNodeConfig wrongSecret = config("codec-b", "different-codec-secret", 19302, 19402);
        assertNull(ClusterMessageCodec.decode(wrongSecret, encoded, encoded.length));
        assertNull(ClusterMessageCodec.decode(sender, encoded, 8));
    }

    @Test
    void routeChunkCountIsBoundedBeforeEncoding() {
        RegistryNodeConfig config = config("codec-a", "codec-shared-secret", 19301, 19401);
        List<ClusterRoute> routes = java.util.Collections.nCopies(
                ClusterMessageCodec.MAX_ROUTES_PER_CHUNK + 1,
                ClusterRoute.action("codec-a", config.epoch(), 1, 1L, "Service", 1, 7, 1L));

        assertThrows(IllegalArgumentException.class,
                () -> ClusterMessageCodec.encodeGossip(config, List.of(), routes, 1L, 0, 1));
    }

    @Test
    void discoveryCarriesRegistryAddresses() {
        System.setProperty("kynet.broker.clientHost", "10.20.30.40");
        RegistryNodeConfig config = config("codec-a", "codec-shared-secret", 19301, 19401);
        byte[] encoded = ClusterMessageCodec.encodeDiscovery(config, null);

        ClusterMessageCodec.DiscoveryMessage decoded = assertInstanceOf(
                ClusterMessageCodec.DiscoveryMessage.class,
                ClusterMessageCodec.decode(config, encoded, encoded.length));

        assertEquals("127.0.0.1", decoded.host());
        assertEquals("10.20.30.40", decoded.clientHost());
    }

    @Test
    void gossipPreservesRegistrationEndpointsAndClientAddresses() {
        RegistryNodeConfig config = config("codec-a", "codec-shared-secret", 19301, 19401);
        RegistryClusterRouteTable table = new RegistryClusterRouteTable(config.nodeId(), config.epoch());
        InstanceMeta meta = new InstanceMeta("Service", 1, 17, 1L);
        table.upEndpoint(meta, "aeron:udp?endpoint=127.0.0.1:12000", 1010, 42L);
        table.upAction(meta, 7);
        table.upTopic(meta, "Aa");
        table.upTopic(meta, "BB");
        RegistryMember member = new RegistryMember(config.nodeId(), config.epoch(), "10.1.1.1", "10.2.2.2",
                19301, 19401, 1, MemberState.ALIVE, 1, 0);
        byte[] encoded = ClusterMessageCodec.encodeGossip(config, List.of(member), table.localSnapshots(), 4L, 0, 1);
        ClusterMessageCodec.GossipMessage decoded = assertInstanceOf(ClusterMessageCodec.GossipMessage.class,
                ClusterMessageCodec.decode(config, encoded, encoded.length));
        assertEquals(table.localSnapshots(), decoded.routes());
        assertEquals("10.2.2.2", decoded.members().getFirst().clientHost());
    }

    @Test
    void rejectsRemovedRelayTypesAndPreviousProtocolVersionEvenWithValidMac() {
        RegistryNodeConfig config = config("codec-a", "codec-shared-secret", 19301, 19401);
        byte[] encoded = ClusterMessageCodec.encodeDiscovery(config, null);
        for (int type : new int[]{3, 4}) {
            byte[] invalid = encoded.clone();
            invalid[5] = (byte) type;
            System.arraycopy(config.computeProtocolMac(invalid, invalid.length - 32), 0, invalid, invalid.length - 32, 32);
            assertNull(ClusterMessageCodec.decode(config, invalid, invalid.length));
        }
        encoded[4] = 3;
        System.arraycopy(config.computeProtocolMac(encoded, encoded.length - 32), 0, encoded, encoded.length - 32, 32);
        assertNull(ClusterMessageCodec.decode(config, encoded, encoded.length));
    }

    @Test
    void rejectsAuthenticatedMessagesOutsideClockWindowAndCarriesUniqueNonce() {
        System.setProperty("kynet.broker.cluster.protocolClockSkewMs", "100");
        RegistryNodeConfig config = config("codec-a", "codec-shared-secret", 19301, 19401);
        byte[] first = ClusterMessageCodec.encodeDiscovery(config, null);
        byte[] second = ClusterMessageCodec.encodeDiscovery(config, null);
        long sentAtMillis = ByteBuffer.wrap(first).getLong(6);

        ClusterMessageCodec.ClusterMessage decoded =
                ClusterMessageCodec.decode(config, first, first.length, sentAtMillis + 100L);
        assertNotNull(decoded);
        assertNull(ClusterMessageCodec.decode(config, first, first.length, sentAtMillis + 101L));
        assertNull(ClusterMessageCodec.decode(config, first, first.length, sentAtMillis - 101L));
        ClusterMessageCodec.ClusterMessage secondDecoded =
                ClusterMessageCodec.decode(config, second, second.length);
        assertNotNull(secondDecoded);
        assertNotEquals(decoded.nonce(), secondDecoded.nonce());
    }

    @Test
    void eachBrokerConfigOwnsItsNonceSequence() {
        RegistryNodeConfig firstNode = config("codec-a", "codec-shared-secret", 19301, 19401);
        RegistryNodeConfig secondNode = config("codec-b", "codec-shared-secret", 19302, 19402);

        long firstA = decodeNonce(firstNode, ClusterMessageCodec.encodeDiscovery(firstNode, null));
        long firstB = decodeNonce(secondNode, ClusterMessageCodec.encodeDiscovery(secondNode, null));
        long secondB = decodeNonce(secondNode, ClusterMessageCodec.encodeDiscovery(secondNode, null));
        long secondA = decodeNonce(firstNode, ClusterMessageCodec.encodeDiscovery(firstNode, null));

        assertEquals(firstA + 1L, secondA);
        assertEquals(firstB + 1L, secondB);
    }

    private static long decodeNonce(RegistryNodeConfig config, byte[] encoded) {
        ClusterMessageCodec.ClusterMessage decoded = ClusterMessageCodec.decode(config, encoded, encoded.length);
        assertNotNull(decoded);
        return decoded.nonce();
    }

    private static RegistryNodeConfig config(String nodeId, String secret, int clientPort, int clusterPort) {
        System.setProperty("kynet.broker.cluster.enabled", "true");
        System.setProperty("kynet.broker.cluster.secret", secret);
        System.setProperty("kynet.broker.nodeId", nodeId);
        System.setProperty("kynet.broker.clusterHost", "127.0.0.1");
        System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPort));
        return RegistryNodeConfig.fromSystemProperties(clientPort);
    }
}
