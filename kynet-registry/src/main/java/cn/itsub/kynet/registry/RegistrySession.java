package cn.itsub.kynet.registry;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.agrona.DirectBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/** A registration bound to one TCP connection with a bounded control queue. */
final class RegistrySession implements AutoCloseable {
    enum DeliveryResult { SENT, BUFFERED, BACK_PRESSURED, CLOSED }
    private final Channel channel;
    private final InstanceMeta meta;
    private final int capacity;
    private final LongAdder sent;
    private final Queue<byte[]> pending = new ArrayDeque<>();
    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private int pendingBytes;
    private volatile long lastSeenNanos = System.nanoTime();

    RegistrySession(Channel channel, InstanceMeta meta, int capacity, LongAdder sent) {
        this.channel = channel;
        this.meta = meta;
        this.capacity = Math.max(64 * 1024, capacity);
        this.sent = sent;
    }

    synchronized DeliveryResult send(DirectBuffer buffer, int offset, int length) {
        if (!channel.isActive()) return DeliveryResult.CLOSED;
        if (length <= 0 || length > RegistryServer.MAX_FRAME_BYTES - Integer.BYTES) return DeliveryResult.BACK_PRESSURED;
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        if (pending.isEmpty() && channel.eventLoop().inEventLoop() && channel.isWritable()) {
            write(bytes);
            channel.flush();
            return DeliveryResult.SENT;
        }
        if (pendingBytes + length > capacity) return DeliveryResult.BACK_PRESSURED;
        pending.add(bytes);
        pendingBytes += length;
        scheduleDrain();
        return DeliveryResult.BUFFERED;
    }

    private void write(byte[] bytes) {
        channel.write(Unpooled.wrappedBuffer(bytes)).addListener(future -> {
            if (future.isSuccess()) sent.increment();
            else channel.close();
        });
    }

    void onWritable() { scheduleDrain(); }

    private void scheduleDrain() {
        if (!drainScheduled.compareAndSet(false, true)) return;
        try {
            channel.eventLoop().execute(this::drain);
        } catch (RuntimeException e) {
            drainScheduled.set(false);
            channel.close();
        }
    }

    private void drain() {
        boolean wrote = false;
        while (channel.isActive() && channel.isWritable()) {
            byte[] bytes;
            synchronized (this) {
                bytes = pending.poll();
                if (bytes != null) pendingBytes -= bytes.length;
            }
            if (bytes == null) break;
            write(bytes);
            wrote = true;
        }
        if (wrote) channel.flush();
        drainScheduled.set(false);
        synchronized (this) {
            if (!pending.isEmpty() && channel.isActive() && channel.isWritable()) scheduleDrain();
        }
    }

    Channel channel() { return channel; }
    InstanceMeta instanceMeta() { return meta; }
    void touch() { lastSeenNanos = System.nanoTime(); }
    boolean isExpired(long now, long timeout) { return now - lastSeenNanos > timeout; }
    @Override public synchronized void close() {
        pending.clear();
        pendingBytes = 0;
        channel.close();
    }
}
