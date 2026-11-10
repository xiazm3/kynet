package cn.itsub.kynet.registry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthenticatedReplayWindowTest {
    @Test
    void rejectsReplayAndStaleNonceWithBoundedMemory() {
        AuthenticatedReplayWindow window = new AuthenticatedReplayWindow(4);

        assertEquals(AuthenticatedReplayWindow.Admission.FIRST_SEEN, window.admit(-3L, 1L));
        assertEquals(AuthenticatedReplayWindow.Admission.FIRST_SEEN, window.admit(-1L, 2L));
        assertEquals(AuthenticatedReplayWindow.Admission.FIRST_SEEN, window.admit(-2L, 3L));
        assertEquals(AuthenticatedReplayWindow.Admission.REPLAY, window.admit(-2L, 4L));
        assertEquals(AuthenticatedReplayWindow.Admission.FIRST_SEEN, window.admit(1L, 5L));
        assertEquals(AuthenticatedReplayWindow.Admission.OUTSIDE_WINDOW, window.admit(-3L, 6L));
        assertTrue(window.size() <= 4);
    }

    @Test
    void expiresAfterAuthenticationWindow() {
        AuthenticatedReplayWindow window = new AuthenticatedReplayWindow(4);
        window.admit(10L, 100L);

        assertTrue(window.isExpired(201L, 100L));
    }
}
