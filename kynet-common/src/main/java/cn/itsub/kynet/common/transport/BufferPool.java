package cn.itsub.kynet.common.transport;

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public final class BufferPool {
    private static final int DEFAULT_BUFFER_SIZE = 16 * 1024;
    private static final int MAX_SHARED_BUFFERS = 256;

    private static final ThreadLocal<UnsafeBuffer> THREAD_LOCAL_BUFFER = ThreadLocal.withInitial(
            () -> new UnsafeBuffer(ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE)));
    // Bounded and allocation-free per op. A ConcurrentLinkedQueue allocated a node on every
    // release and made size() O(n) on the RPC hot path.
    private static final ManyToManyConcurrentArrayQueue<UnsafeBuffer> SHARED_BUFFERS =
            new ManyToManyConcurrentArrayQueue<>(MAX_SHARED_BUFFERS);

    private BufferPool() {
    }

    public static UnsafeBuffer acquire() {
        return THREAD_LOCAL_BUFFER.get();
    }

    public static UnsafeBuffer acquireShared(int minCapacity) {
        UnsafeBuffer buffer;
        int inspected = 0;
        while ((buffer = SHARED_BUFFERS.poll()) != null) {
            if (buffer.capacity() >= minCapacity) {
                return buffer;
            }
            SHARED_BUFFERS.offer(buffer);
            if (++inspected >= MAX_SHARED_BUFFERS) {
                break;
            }
        }
        return new UnsafeBuffer(ByteBuffer.allocateDirect(normalizeCapacity(minCapacity)));
    }

    public static void releaseShared(UnsafeBuffer buffer) {
        if (buffer != null) {
            SHARED_BUFFERS.offer(buffer);
        }
    }

    private static int normalizeCapacity(int minCapacity) {
        int capacity = DEFAULT_BUFFER_SIZE;
        while (capacity < minCapacity) {
            capacity <<= 1;
        }
        return capacity;
    }
}
