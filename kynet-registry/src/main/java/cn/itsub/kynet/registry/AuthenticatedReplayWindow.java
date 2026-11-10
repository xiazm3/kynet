package cn.itsub.kynet.registry;

final class AuthenticatedReplayWindow {
    private final long[] nonces;
    private final boolean[] occupied;
    private boolean initialized;
    private long highestNonce;
    private volatile long lastSeenMillis;
    private int size;

    AuthenticatedReplayWindow(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        nonces = new long[capacity];
        occupied = new boolean[capacity];
    }

    synchronized Admission admit(long nonce, long nowMillis) {
        lastSeenMillis = nowMillis;
        int index = (int) Math.floorMod(nonce, (long) nonces.length);
        if (occupied[index] && nonces[index] == nonce) {
            return Admission.REPLAY;
        }
        if (initialized && nonce <= highestNonce && outsideWindow(nonce)) {
            return Admission.OUTSIDE_WINDOW;
        }
        if (!occupied[index]) {
            size++;
        }
        nonces[index] = nonce;
        occupied[index] = true;
        if (!initialized || nonce > highestNonce) {
            initialized = true;
            highestNonce = nonce;
        }
        return Admission.FIRST_SEEN;
    }

    private boolean outsideWindow(long nonce) {
        try {
            return Math.subtractExact(highestNonce, nonce) >= nonces.length;
        } catch (ArithmeticException ignored) {
            return true;
        }
    }

    synchronized int size() {
        return size;
    }

    boolean isExpired(long nowMillis, long ttlMillis) {
        return nowMillis - lastSeenMillis >= ttlMillis;
    }

    enum Admission {
        FIRST_SEEN,
        REPLAY,
        OUTSIDE_WINDOW
    }
}
