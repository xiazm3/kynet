package cn.itsub.kynet.framework.gateway;

final class SessionIdGenerator {
    private static final long CUSTOM_EPOCH_MILLIS = 1_704_067_200_000L; // 2024-01-01T00:00:00Z
    private static final int WORKER_ID_BITS = 10;
    private static final int SEQUENCE_BITS = 12;
    private static final long MAX_WORKER_ID = (1L << WORKER_ID_BITS) - 1;
    private static final long SEQUENCE_MASK = (1L << SEQUENCE_BITS) - 1;

    private final long workerId;
    private long lastTimestamp = -1L;
    private long sequence;

    SessionIdGenerator(long workerId) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("workerId must be in range 0.." + MAX_WORKER_ID);
        }
        this.workerId = workerId;
    }

    synchronized long nextId() {
        long timestamp = currentTimestamp();
        if (timestamp < lastTimestamp) {
            timestamp = waitUntil(lastTimestamp);
        }
        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                timestamp = waitUntil(lastTimestamp + 1);
            }
        } else {
            sequence = 0;
        }
        lastTimestamp = timestamp;
        return ((timestamp - CUSTOM_EPOCH_MILLIS) << (WORKER_ID_BITS + SEQUENCE_BITS))
                | (workerId << SEQUENCE_BITS)
                | sequence;
    }

    static long resolveWorkerId(int serviceNodeInstanceId) {
        String configured = System.getProperty("kynet.gateway.workerId");
        if (configured != null && !configured.isBlank()) {
            long workerId = Long.parseLong(configured);
            if (workerId < 0 || workerId > MAX_WORKER_ID) {
                throw new IllegalArgumentException("kynet.gateway.workerId must be in range 0.." + MAX_WORKER_ID);
            }
            return workerId;
        }
        return Integer.toUnsignedLong(mix(serviceNodeInstanceId)) & MAX_WORKER_ID;
    }

    private static int mix(int value) {
        value ^= value >>> 16;
        value *= 0x7feb352d;
        value ^= value >>> 15;
        value *= 0x846ca68b;
        value ^= value >>> 16;
        return value;
    }

    private long waitUntil(long targetTimestamp) {
        long timestamp;
        do {
            Thread.onSpinWait();
            timestamp = currentTimestamp();
        } while (timestamp < targetTimestamp);
        return timestamp;
    }

    private long currentTimestamp() {
        return System.currentTimeMillis();
    }
}
