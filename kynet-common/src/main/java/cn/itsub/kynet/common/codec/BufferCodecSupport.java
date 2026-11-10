package cn.itsub.kynet.common.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.charset.StandardCharsets;

final class BufferCodecSupport {
    private BufferCodecSupport() {
    }

    static int writeShortString(MutableDirectBuffer buffer, int offset, String value) {
        if (value == null || value.isEmpty()) {
            buffer.putShort(offset, (short) 0);
            return offset + 2;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.putShort(offset, (short) bytes.length);
        buffer.putBytes(offset + 2, bytes);
        return offset + 2 + bytes.length;
    }

    static int writeIntBytes(MutableDirectBuffer buffer, int offset, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            buffer.putInt(offset, 0);
            return offset + 4;
        }
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + 4, bytes);
        return offset + 4 + bytes.length;
    }

    static int writeIntBytes(MutableDirectBuffer buffer, int offset,
                             DirectBuffer srcBuffer, int srcOffset, int length) {
        if (srcBuffer == null || length <= 0) {
            buffer.putInt(offset, 0);
            return offset + 4;
        }
        buffer.putInt(offset, length);
        buffer.putBytes(offset + 4, srcBuffer, srcOffset, length);
        return offset + 4 + length;
    }

    static String readString(DirectBuffer buffer, int dataOffset, int length) {
        if (buffer == null || length <= 0) {
            return null;
        }
        return buffer.getStringWithoutLengthUtf8(dataOffset, length);
    }

    static ShortStringField readShortStringField(DirectBuffer buffer, int offset, int limit) {
        if (offset + 2 > limit) {
            return new ShortStringField(offset, 0, offset);
        }
        int length = buffer.getShort(offset) & 0xFFFF;
        int dataOffset = offset + 2;
        int nextOffset = Math.min(limit, dataOffset + length);
        return new ShortStringField(dataOffset, length, nextOffset);
    }

    static IntBytesField readIntBytesField(DirectBuffer buffer, int offset, int limit) {
        if (offset + 4 > limit) {
            return new IntBytesField(offset, 0, offset);
        }
        int length = Math.max(0, buffer.getInt(offset));
        int dataOffset = offset + 4;
        int nextOffset = Math.min(limit, dataOffset + length);
        return new IntBytesField(dataOffset, length, nextOffset);
    }

    record ShortStringField(int dataOffset, int length, int nextOffset) {
        String decode(DirectBuffer buffer) {
            return BufferCodecSupport.readString(buffer, dataOffset, length);
        }
    }

    record IntBytesField(int dataOffset, int length, int nextOffset) {
        byte[] copy(DirectBuffer buffer) {
            byte[] bytes = new byte[length];
            if (length > 0) {
                buffer.getBytes(dataOffset, bytes);
            }
            return bytes;
        }
    }
}
