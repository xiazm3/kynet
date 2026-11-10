package cn.itsub.kynet.registry;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RegistryClusterRouteTableTest {
    @Test void ownerSnapshotRemovesUnregisteredEntriesAndRejectsOlderVersions() {
        RegistryClusterRouteTable table = new RegistryClusterRouteTable("local", 1);
        ClusterRoute route = ClusterRoute.action("remote", 2, 77, 1, "Service", 7, 99, 1);
        assertTrue(table.replaceRemoteSnapshot("remote", 2, 1, List.of(route)));
        assertEquals(List.of(route), table.snapshots());
        assertTrue(table.replaceRemoteSnapshot("remote", 2, 2, List.of()));
        assertFalse(table.replaceRemoteSnapshot("remote", 2, 1, List.of(route)));
        assertTrue(table.snapshots().isEmpty());
    }
    @Test void instanceRemovalClearsEndpointActionsAndFullTopicNames() {
        RegistryClusterRouteTable table = new RegistryClusterRouteTable("local", 1);
        InstanceMeta meta = new InstanceMeta("Service", 7, 77, 1);
        table.upEndpoint(meta, "aeron:udp?endpoint=127.0.0.1:1234", 1010, 9);
        table.upAction(meta, 99);
        table.upTopic(meta, "Aa");
        table.upTopic(meta, "BB");
        assertEquals(4, table.localSnapshots().size());
        table.downInstance(77);
        assertTrue(table.localSnapshots().isEmpty());
    }
    @Test void removesOnlyTheDepartedRegistryIncarnation() {
        RegistryClusterRouteTable table = new RegistryClusterRouteTable("local", 1);
        ClusterRoute old = ClusterRoute.action("remote", 2, 77, 1, "Service", 7, 99, 1);
        ClusterRoute current = ClusterRoute.action("remote", 3, 77, 2, "Service", 7, 99, 1);
        assertTrue(table.replaceRemoteSnapshot("remote", 2, 10, List.of(old)));
        assertTrue(table.replaceRemoteSnapshot("remote", 3, 1, List.of(current)));
        table.removeRoutesForNode("remote", 2);
        assertEquals(List.of(current), table.snapshots());
        table.removeRoutesForNode("remote", 3);
        assertTrue(table.snapshots().isEmpty());
    }
    @Test void rejectsRoutesClaimingAnotherRegistryOwner() {
        RegistryClusterRouteTable table = new RegistryClusterRouteTable("local", 1);
        ClusterRoute route = ClusterRoute.action("other", 2, 77, 1, "Service", 7, 99, 1);
        assertFalse(table.replaceRemoteSnapshot("remote", 2, 1, List.of(route)));
        assertTrue(table.snapshots().isEmpty());
    }
}
