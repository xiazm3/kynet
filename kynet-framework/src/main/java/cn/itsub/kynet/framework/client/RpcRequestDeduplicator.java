package cn.itsub.kynet.framework.client;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

final class RpcRequestDeduplicator {
    private static final CachedResponse IN_FLIGHT_TIMEOUT_RESPONSE = new CachedResponse(
            false,
            "IDEMPOTENCY_IN_FLIGHT_TIMEOUT",
            "RPC action exceeded the idempotency in-flight timeout",
            false,
            null,
            null);

    private final int maxEntries;
    private final long ttlNanos;
    private final long inFlightTtlNanos;
    private final Map<RequestKey, Entry> entries = new ConcurrentHashMap<>();
    private final Object admissionLock = new Object();

    RpcRequestDeduplicator(int maxEntries, Duration ttl) {
        this(maxEntries, ttl, ttl);
    }

    RpcRequestDeduplicator(int maxEntries, Duration ttl, Duration inFlightTtl) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
        if (inFlightTtl == null || inFlightTtl.isZero() || inFlightTtl.isNegative()) {
            throw new IllegalArgumentException("inFlightTtl must be positive");
        }
        this.maxEntries = maxEntries;
        this.ttlNanos = ttl.toNanos();
        this.inFlightTtlNanos = inFlightTtl.toNanos();
    }

    AcquireResult acquire(int sourceInstanceId, long requestId, int action) {
        return acquire(sourceInstanceId, 0L, requestId, action);
    }

    AcquireResult acquire(int sourceInstanceId, long incarnation, long requestId, int action) {
        RequestKey key = new RequestKey(sourceInstanceId, incarnation, requestId);
        long nowNanos = System.nanoTime();
        Entry existing = entries.get(key);
        expireInFlight(existing, nowNanos);
        if (existing != null && !removeIfExpired(key, existing, nowNanos)) {
            return existing.action == action
                    ? new AcquireResult(AcquireStatus.DUPLICATE, existing)
                    : new AcquireResult(AcquireStatus.CONFLICT, existing);
        }

        synchronized (admissionLock) {
            existing = entries.get(key);
            expireInFlight(existing, nowNanos);
            if (existing != null && !removeIfExpired(key, existing, nowNanos)) {
                return existing.action == action
                        ? new AcquireResult(AcquireStatus.DUPLICATE, existing)
                        : new AcquireResult(AcquireStatus.CONFLICT, existing);
            }
            if (entries.size() >= maxEntries) {
                entries.entrySet().removeIf(entry -> {
                    Entry value = entry.getValue();
                    expireInFlight(value, nowNanos);
                    return value.isExpired(nowNanos);
                });
                if (entries.size() >= maxEntries) {
                    return new AcquireResult(AcquireStatus.CAPACITY_EXCEEDED, null);
                }
            }
            Entry created = new Entry(key, action, nowNanos, ttlNanos, inFlightTtlNanos);
            entries.put(key, created);
            return new AcquireResult(AcquireStatus.OWNER, created);
        }
    }

    void discard(Entry entry) {
        if (entry != null) {
            entries.remove(entry.key, entry);
        }
    }

    void clear() {
        entries.clear();
    }

    int size() {
        return entries.size();
    }

    private boolean removeIfExpired(RequestKey key, Entry entry, long nowNanos) {
        return entry.isExpired(nowNanos) && entries.remove(key, entry);
    }

    private void expireInFlight(Entry entry, long nowNanos) {
        if (entry != null) {
            entry.expireInFlight(nowNanos);
        }
    }

    enum AcquireStatus {
        OWNER,
        DUPLICATE,
        CONFLICT,
        CAPACITY_EXCEEDED
    }

    record AcquireResult(AcquireStatus status, Entry entry) {
    }

    static final class Entry {
        private final RequestKey key;
        private final int action;
        private final long startedAtNanos;
        private final long ttlNanos;
        private final long inFlightTtlNanos;
        private final CompletableFuture<CachedResponse> response = new CompletableFuture<>();
        private volatile long expiresAtNanos = Long.MAX_VALUE;

        private Entry(RequestKey key, int action, long startedAtNanos,
                      long ttlNanos, long inFlightTtlNanos) {
            this.key = key;
            this.action = action;
            this.startedAtNanos = startedAtNanos;
            this.ttlNanos = ttlNanos;
            this.inFlightTtlNanos = inFlightTtlNanos;
        }

        CompletableFuture<CachedResponse> response() {
            return response;
        }

        boolean complete(CachedResponse cachedResponse) {
            long nowNanos = System.nanoTime();
            expireInFlight(nowNanos);
            return complete(cachedResponse, nowNanos);
        }

        private void expireInFlight(long nowNanos) {
            if (!response.isDone() && nowNanos - startedAtNanos >= inFlightTtlNanos) {
                complete(IN_FLIGHT_TIMEOUT_RESPONSE, startedAtNanos + inFlightTtlNanos);
            }
        }

        private boolean complete(CachedResponse cachedResponse, long nowNanos) {
            if (response.complete(cachedResponse)) {
                expiresAtNanos = nowNanos + ttlNanos;
                return true;
            }
            return false;
        }

        private boolean isExpired(long nowNanos) {
            return response.isDone() && nowNanos - expiresAtNanos >= 0L;
        }
    }

    record CachedResponse(boolean success, String errorCode, String errorMessage,
                          boolean attachmentUpdated,
                          byte[] attachmentData, byte[] body) {
        CachedResponse {
            attachmentData = copy(attachmentData);
            body = copy(body);
        }

        private static byte[] copy(byte[] value) {
            return value == null ? null : Arrays.copyOf(value, value.length);
        }
    }

    private record RequestKey(int sourceInstanceId, long incarnation, long requestId) {
    }
}
