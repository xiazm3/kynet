package cn.itsub.kynet.registry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryMemberTableTest {
    private static final String[] PROPERTIES = {
            "kynet.broker.cluster.enabled",
            "kynet.broker.cluster.secret",
            "kynet.broker.nodeId",
            "kynet.broker.clientHost",
            "kynet.broker.clusterHost",
            "kynet.broker.clusterBindHost",
            "kynet.broker.clusterPort",
            "kynet.broker.suspectTimeoutMs",
            "kynet.broker.deadTimeoutMs"
    };

    @AfterEach
    void clearProperties() {
        for (String property : PROPERTIES) {
            System.clearProperty(property);
        }
    }

    @Test
    void transitiveAliveRumorsDoNotKeepDeadNodeAlive() {
        RegistryNodeConfig config = config("member-a", 19101, 19201);
        RegistryMemberTable table = new RegistryMemberTable(config);
        long epoch = System.currentTimeMillis();
        assertTrue(table.mergeDiscovery(new ClusterMessageCodec.DiscoveryMessage(
                config.clusterName(), "member-c", epoch, "127.0.0.1",
                19103, 19203, 1L, 1L)));
        RegistryMember staleAlive = new RegistryMember(
                "member-c", epoch, "127.0.0.1", 19103, 19203,
                1L, MemberState.ALIVE, 1L, 0L);

        long lastDirectSeen = table.member("member-c").lastSeenMillis();
        table.mergeRumors(List.of(staleAlive));
        table.mergeRumors(List.of(staleAlive));
        assertEquals(lastDirectSeen, table.member("member-c").lastSeenMillis());

        assertTrue(table.detectFailures(lastDirectSeen + config.suspectTimeoutMillis()));
        assertEquals(MemberState.SUSPECT, table.member("member-c").state());
        assertTrue(table.detectFailures(lastDirectSeen + config.deadTimeoutMillis()));
        assertEquals(MemberState.DEAD, table.member("member-c").state());
        assertTrue(table.topologyEndpoints().stream()
                .anyMatch(endpoint -> "member-c".equals(endpoint.nodeId())
                        && "DEAD".equals(endpoint.state())));
    }

    @Test
    void selfRefutesSuspicionWithHigherIncarnation() {
        RegistryNodeConfig config = config("member-a", 19101, 19201);
        RegistryMemberTable table = new RegistryMemberTable(config);
        RegistryMember accusation = new RegistryMember(
                "member-a", config.epoch(), "127.0.0.1", 19101, 19201,
                7L, MemberState.DEAD, 20L, 0L);

        assertTrue(table.mergeRumors(List.of(accusation)));

        assertEquals(MemberState.ALIVE, table.self().state());
        assertTrue(table.self().incarnation() > accusation.incarnation());
        assertTrue(table.self().version() > accusation.version());
    }

    @Test
    void topologyAdvertisesClientHostInsteadOfClusterHost() {
        RegistryNodeConfig config = config("member-a", 19101, 19201);
        RegistryMemberTable table = new RegistryMemberTable(config);

        assertTrue(table.mergeDiscovery(new ClusterMessageCodec.DiscoveryMessage(
                config.clusterName(), "member-b", System.currentTimeMillis(),
                "10.0.0.2", "192.168.0.2", 19102, 19202,
                1L, 1L, 0L)));

        assertTrue(table.topologyEndpoints().stream()
                .anyMatch(endpoint -> "member-b".equals(endpoint.nodeId())
                        && "192.168.0.2".equals(endpoint.host())));
    }

    private static RegistryNodeConfig config(String nodeId, int clientPort, int clusterPort) {
        System.setProperty("kynet.broker.cluster.enabled", "true");
        System.setProperty("kynet.broker.cluster.secret", "member-table-test-secret");
        System.setProperty("kynet.broker.nodeId", nodeId);
        System.setProperty("kynet.broker.clusterHost", "127.0.0.1");
        System.setProperty("kynet.broker.clusterBindHost", "127.0.0.1");
        System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPort));
        System.setProperty("kynet.broker.suspectTimeoutMs", "100");
        System.setProperty("kynet.broker.deadTimeoutMs", "200");
        return RegistryNodeConfig.fromSystemProperties(clientPort);
    }
}
