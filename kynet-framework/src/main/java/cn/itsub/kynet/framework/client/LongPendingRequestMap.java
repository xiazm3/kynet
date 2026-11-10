package cn.itsub.kynet.framework.client;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongObjConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Pending-request map keyed by primitive request id.
 *
 * <p>{@code ConcurrentHashMap<Long, ServiceNode.PendingRequest>} allocated a {@code Long} and a map node on
 * every RPC on the hot path. This uses striped Agrona primitive maps instead, which keeps the
 * request/response path allocation-free for the map itself. Stripes are hashed by the request
 * id's low bits, which advance per sender, so concurrent senders land on different stripes.
 */
final class LongPendingRequestMap {
    private static final int STRIPES = 16;

    private final Long2ObjectHashMap<ServiceNode.PendingRequest>[] maps;
    private final ReentrantLock[] locks;
    private final AtomicInteger size = new AtomicInteger();

    @SuppressWarnings("unchecked")
    LongPendingRequestMap() {
        maps = new Long2ObjectHashMap[STRIPES];
        locks = new ReentrantLock[STRIPES];
        for (int i = 0; i < STRIPES; i++) {
            maps[i] = new Long2ObjectHashMap<>();
            locks[i] = new ReentrantLock();
        }
    }

    private static int stripe(long key) {
        return (int) key & (STRIPES - 1);
    }

    void put(long key, ServiceNode.PendingRequest value) {
        int index = stripe(key);
        ReentrantLock lock = locks[index];
        lock.lock();
        try {
            if (maps[index].put(key, value) == null) {
                size.incrementAndGet();
            }
        } finally {
            lock.unlock();
        }
    }

    ServiceNode.PendingRequest get(long key) {
        int index = stripe(key);
        ReentrantLock lock = locks[index];
        lock.lock();
        try {
            return maps[index].get(key);
        } finally {
            lock.unlock();
        }
    }

    boolean remove(long key, ServiceNode.PendingRequest expected) {
        int index = stripe(key);
        ReentrantLock lock = locks[index];
        lock.lock();
        try {
            if (maps[index].get(key) != expected) {
                return false;
            }
            maps[index].remove(key);
            size.decrementAndGet();
            return true;
        } finally {
            lock.unlock();
        }
    }

    boolean isEmpty() {
        return size.get() == 0;
    }

    /**
     * Snapshots entries before invoking the consumer. Timeout and shutdown paths remove entries
     * from the consumer, which would invalidate an in-place iteration.
     */
    void forEach(LongObjConsumer<ServiceNode.PendingRequest> consumer) {
        List<Long> keys = new ArrayList<>();
        List<ServiceNode.PendingRequest> values = new ArrayList<>();
        for (int i = 0; i < STRIPES; i++) {
            ReentrantLock lock = locks[i];
            lock.lock();
            try {
                maps[i].forEachLong((key, sp) -> {
                    keys.add(key);
                    values.add(sp);
                });
            } finally {
                lock.unlock();
            }
        }
        for (int i = 0; i < keys.size(); i++) {
            consumer.accept(keys.get(i), values.get(i));
        }
    }
}
