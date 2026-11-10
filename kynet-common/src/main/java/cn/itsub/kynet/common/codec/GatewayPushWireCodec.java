package cn.itsub.kynet.common.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public final class GatewayPushWireCodec {
    private GatewayPushWireCodec() {
    }

    public static int encode(MutableDirectBuffer buffer, int offset,
                             long sessionId, long userId, int cmd, int act, byte[] payload) {
        int pos = offset;
        buffer.putLong(pos, sessionId);
        pos += 8;
        buffer.putLong(pos, userId);
        pos += 8;
        buffer.putInt(pos, cmd);
        pos += 4;
        buffer.putInt(pos, act);
        pos += 4;
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, payload);
        return pos - offset;
    }

    public static GatewayPushView decodeView(DirectBuffer buffer, int offset, int length) {
        int limit = offset + length;
        long sessionId = offset + 8 <= limit ? buffer.getLong(offset) : 0L;
        int pos = offset + 8;
        long userId = pos + 8 <= limit ? buffer.getLong(pos) : 0L;
        pos += 8;
        int cmd = pos + 4 <= limit ? buffer.getInt(pos) : 0;
        pos += 4;
        int act = pos + 4 <= limit ? buffer.getInt(pos) : 0;
        pos += 4;
        BufferCodecSupport.IntBytesField payload = BufferCodecSupport.readIntBytesField(buffer, pos, limit);
        return new GatewayPushView(buffer, sessionId, userId, cmd, act, payload);
    }

    public record GatewayPushView(
            DirectBuffer buffer,
            long sessionId,
            long userId,
            int cmd,
            int act,
            BufferCodecSupport.IntBytesField payloadField
    ) {
        public DirectBuffer payloadBuffer() {
            return buffer;
        }

        public int payloadOffset() {
            return payloadField.dataOffset();
        }

        public int payloadLength() {
            return payloadField.length();
        }

        public byte[] copyPayload() {
            return payloadField.copy(buffer);
        }
    }
}
