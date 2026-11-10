package cn.itsub.kynet.framework.gateway.netty;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayOutboundStateTest {

    @Test
    void pendingLimitsAreHardBoundsAndFailedAcquireDoesNotConsumeCapacity() {
        GatewayOutboundState state = new GatewayOutboundState();

        assertTrue(state.tryAcquire(80, 100, 2));
        assertFalse(state.tryAcquire(30, 100, 2));
        assertEquals(80L, state.pendingBytes());
        assertEquals(1, state.pendingMessages());

        assertTrue(state.tryAcquire(20, 100, 2));
        assertFalse(state.tryAcquire(0, 100, 2));
        assertEquals(100L, state.pendingBytes());
        assertEquals(2, state.pendingMessages());

        state.release(80);
        assertEquals(20L, state.pendingBytes());
        assertEquals(1, state.pendingMessages());
        state.release(20);
        assertEquals(0L, state.pendingBytes());
        assertEquals(0, state.pendingMessages());
    }

    @Test
    void unwritableTimeoutIsClearedAfterRecovery() {
        GatewayOutboundState state = new GatewayOutboundState();
        long start = 1_000L;

        assertTrue(state.markUnwritable(start));
        assertFalse(state.markUnwritable(start + 1L));
        assertFalse(state.isUnwritableTimedOut(start + 999L, 1_000L));
        assertTrue(state.isUnwritableTimedOut(start + 1_000L, 1_000L));

        state.markWritable();
        assertFalse(state.isUnwritableTimedOut(start + 10_000L, 1_000L));
        assertEquals(0L, state.unwritableSinceNanos());
    }
}
