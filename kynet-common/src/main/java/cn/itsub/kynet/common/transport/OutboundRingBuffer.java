package cn.itsub.kynet.common.transport;

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class OutboundRingBuffer {
    private static final int DEFAULT_CAPACITY = 4 * 1024 * 1024;
    private static final int ENTRY_HEADER_SIZE = 4;

    private final UnsafeBuffer buffer;
    private final int capacity;
    private int writePos;
    private int readPos;
    private int count;

    public OutboundRingBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public OutboundRingBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(capacity));
    }

    public boolean write(DirectBuffer srcBuffer, int offset, int length) {
        int entrySize = ENTRY_HEADER_SIZE + length;
        if (entrySize > freeContiguous()) {
            if (entrySize > freeTotal()) {
                return false;
            }
            padAndWrap();
            if (entrySize > freeContiguous()) {
                return false;
            }
        }
        buffer.putInt(writePos, length);
        buffer.putBytes(writePos + ENTRY_HEADER_SIZE, srcBuffer, offset, length);
        writePos += entrySize;
        count++;
        return true;
    }

    public int drain(Publication publication) {
        int drained = 0;
        while (count > 0) {
            int length = buffer.getInt(readPos);
            int dataOffset = readPos + ENTRY_HEADER_SIZE;
            long result = publication.offer(buffer, dataOffset, length);
            if (result < 0) {
                break;
            }
            readPos += ENTRY_HEADER_SIZE + length;
            count--;
            drained++;
            if (readPos >= writePos) {
                readPos = 0;
                writePos = 0;
            }
        }
        return drained;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public int size() {
        return count;
    }

    private int freeContiguous() {
        return capacity - writePos;
    }

    private int freeTotal() {
        if (count == 0) {
            return capacity;
        }
        return capacity - (writePos - readPos);
    }

    private void padAndWrap() {
        if (count == 0) {
            readPos = 0;
            writePos = 0;
            return;
        }
        int usedBytes = writePos - readPos;
        if (usedBytes > 0 && readPos > 0) {
            buffer.putBytes(0, buffer, readPos, usedBytes);
            readPos = 0;
            writePos = usedBytes;
        }
    }
}
