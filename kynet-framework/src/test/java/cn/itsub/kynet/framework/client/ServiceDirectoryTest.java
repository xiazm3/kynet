package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.protocol.ServiceDirectoryCodec;
import cn.itsub.kynet.common.protocol.ServiceDirectoryCodec.Endpoint;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ServiceDirectoryTest {
    @Test void commitsOnlyCompleteSnapshotsAndRecoversAcrossRegistryVersions() {
        ServiceDirectory directory = new ServiceDirectory();
        List<Endpoint> endpoints = new ArrayList<>();
        for (int i = 1; i <= 1800; i++) endpoints.add(endpoint(i));
        List<byte[]> chunks = ServiceDirectoryCodec.encode(7, endpoints);
        assertTrue(chunks.size() > 1);
        for (int i = chunks.size() - 1; i > 0; i--) {
            assertFalse(directory.accept(ServiceDirectoryCodec.decode(chunks.get(i))));
        }
        assertEquals(0, directory.endpoints().size());
        assertTrue(directory.accept(ServiceDirectoryCodec.decode(chunks.get(0))));
        assertEquals(1800, directory.endpoints().size());
        assertFalse(directory.accept(ServiceDirectoryCodec.decode(chunks.get(0))));
        directory.reconnect();
        assertEquals(1800, directory.endpoints().size());
        directory.accept(ServiceDirectoryCodec.decode(ServiceDirectoryCodec.encode(1, List.of(endpoint(9))).get(0)));
        assertEquals(1, directory.endpoints().size());
        assertNotNull(directory.select(5, 9, 0));
        assertNull(directory.select(5, 10, 0));
    }

    @Test void rejectsMalformedChunksAndSeparatesHashCollidingTopics() {
        Endpoint endpoint = new Endpoint(1, 2, "service", "aeron:udp?endpoint=127.0.0.1:1234", 1010, Set.of(5), Set.of("Aa"));
        byte[] encoded = ServiceDirectoryCodec.encode(1, List.of(endpoint)).get(0);
        assertThrows(IllegalArgumentException.class, () -> ServiceDirectoryCodec.decode(Arrays.copyOf(encoded, encoded.length - 1)));
        assertEquals("Aa".hashCode(), "BB".hashCode());
        assertFalse(ServiceDirectoryCodec.decode(encoded).endpoints().get(0).topics().contains("BB"));
    }

    @Test void rejectsLateSnapshotsFromPreviousRegistrationInsideCommitLock() {
        ServiceDirectory directory = new ServiceDirectory();
        directory.reconnect(1);
        ServiceDirectoryCodec.Chunk old = ServiceDirectoryCodec.decode(ServiceDirectoryCodec.encode(100, List.of(endpoint(1))).get(0));
        directory.reconnect(2);
        assertFalse(directory.accept(old, 1));
        ServiceDirectoryCodec.Chunk current = ServiceDirectoryCodec.decode(ServiceDirectoryCodec.encode(1, List.of(endpoint(2))).get(0));
        assertTrue(directory.accept(current, 2));
        assertNull(directory.instance(1));
        assertNotNull(directory.instance(2));
    }

    @Test void deduplicationSeparatesCallerIncarnations() {
        RpcRequestDeduplicator dedup = new RpcRequestDeduplicator(10, Duration.ofSeconds(10));
        assertEquals(RpcRequestDeduplicator.AcquireStatus.OWNER, dedup.acquire(1, 10L, 1, 5).status());
        assertEquals(RpcRequestDeduplicator.AcquireStatus.DUPLICATE, dedup.acquire(1, 10L, 1, 5).status());
        assertEquals(RpcRequestDeduplicator.AcquireStatus.OWNER, dedup.acquire(1, 11L, 1, 5).status());
    }

    private Endpoint endpoint(int id) {
        return new Endpoint(id, id, "service", "aeron:udp?endpoint=127.0.0.1:1234", 1010, Set.of(5), Set.of("topic"));
    }
}
