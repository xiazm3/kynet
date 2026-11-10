package cn.itsub.kynet.framework.gateway.netty;

final class GatewayOutboundState {
    private long pendingBytes;
    private int pendingMessages;
    private long unwritableSinceNanos;

    synchronized boolean tryAcquire(int bytes, long maxPendingBytes, int maxPendingMessages) {
        if (bytes < 0) {
            throw new IllegalArgumentException("bytes must not be negative");
        }
        if (pendingBytes > Long.MAX_VALUE - bytes) {
            return false;
        }
        if (pendingMessages == Integer.MAX_VALUE) {
            return false;
        }
        long nextBytes = pendingBytes + bytes;
        int nextMessages = pendingMessages + 1;
        if ((maxPendingBytes > 0 && nextBytes > maxPendingBytes)
                || (maxPendingMessages > 0 && nextMessages > maxPendingMessages)) {
            return false;
        }
        pendingBytes = nextBytes;
        pendingMessages = nextMessages;
        return true;
    }

    synchronized void release(int bytes) {
        pendingBytes = Math.max(0L, pendingBytes - bytes);
        pendingMessages = Math.max(0, pendingMessages - 1);
    }

    synchronized boolean markUnwritable(long nowNanos) {
        if (unwritableSinceNanos != 0L) {
            return false;
        }
        unwritableSinceNanos = nowNanos;
        return true;
    }

    synchronized void markWritable() {
        unwritableSinceNanos = 0L;
    }

    synchronized boolean isUnwritableTimedOut(long nowNanos, long timeoutNanos) {
        return timeoutNanos > 0L
                && unwritableSinceNanos != 0L
                && nowNanos - unwritableSinceNanos >= timeoutNanos;
    }

    synchronized long pendingBytes() {
        return pendingBytes;
    }

    synchronized int pendingMessages() {
        return pendingMessages;
    }

    synchronized long unwritableSinceNanos() {
        return unwritableSinceNanos;
    }
}
