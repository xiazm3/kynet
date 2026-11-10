package cn.itsub.kynet.framework.client;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RpcRequestDeduplicatorTest {
    @Test
    void rejectsNewKeysAtCapacityInsteadOfExecutingWithoutDeduplication() {
        RpcRequestDeduplicator deduplicator = new RpcRequestDeduplicator(1, Duration.ofMinutes(1));

        RpcRequestDeduplicator.AcquireResult first = deduplicator.acquire(7, 11L, 101);
        RpcRequestDeduplicator.AcquireResult full = deduplicator.acquire(7, 12L, 101);

        assertEquals(RpcRequestDeduplicator.AcquireStatus.OWNER, first.status());
        assertEquals(RpcRequestDeduplicator.AcquireStatus.CAPACITY_EXCEEDED, full.status());

        deduplicator.discard(first.entry());
        assertEquals(RpcRequestDeduplicator.AcquireStatus.OWNER,
                deduplicator.acquire(7, 12L, 101).status());
    }

    @Test
    void rejectsRequestIdReuseForAnotherAction() {
        RpcRequestDeduplicator deduplicator = new RpcRequestDeduplicator(10, Duration.ofMinutes(1));

        deduplicator.acquire(7, 11L, 101);

        assertEquals(RpcRequestDeduplicator.AcquireStatus.CONFLICT,
                deduplicator.acquire(7, 11L, 102).status());
    }

    @Test
    void convertsHungOwnerIntoCachedTimeout() throws Exception {
        RpcRequestDeduplicator deduplicator = new RpcRequestDeduplicator(
                10, Duration.ofMinutes(1), Duration.ofMillis(5));
        RpcRequestDeduplicator.AcquireResult owner = deduplicator.acquire(7, 11L, 101);

        TimeUnit.MILLISECONDS.sleep(10);
        RpcRequestDeduplicator.AcquireResult duplicate = deduplicator.acquire(7, 11L, 101);

        assertEquals(RpcRequestDeduplicator.AcquireStatus.DUPLICATE, duplicate.status());
        assertEquals("IDEMPOTENCY_IN_FLIGHT_TIMEOUT",
                duplicate.entry().response().join().errorCode());
        assertFalse(owner.entry().complete(new RpcRequestDeduplicator.CachedResponse(
                true, null, null, false, null, new byte[]{1})));
    }

    @Test
    void eventuallyReclaimsHungEntriesAtCapacity() throws Exception {
        RpcRequestDeduplicator deduplicator = new RpcRequestDeduplicator(
                1, Duration.ofMillis(5), Duration.ofMillis(5));
        deduplicator.acquire(7, 11L, 101);

        TimeUnit.MILLISECONDS.sleep(15);

        assertEquals(RpcRequestDeduplicator.AcquireStatus.OWNER,
                deduplicator.acquire(7, 12L, 101).status());
    }
}
