package cn.itsub.kynet.common.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public final class EventWireCodec {
    private EventWireCodec() {
    }

    public static int encode(MutableDirectBuffer buffer, int offset, String topic, byte[] payload) {
        int pos = offset;
        pos = BufferCodecSupport.writeShortString(buffer, pos, topic);
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, payload);
        return pos - offset;
    }

    public static EventView decodeView(DirectBuffer buffer, int offset, int length) {
        int limit = offset + length;
        BufferCodecSupport.ShortStringField topic = BufferCodecSupport.readShortStringField(buffer, offset, limit);
        BufferCodecSupport.IntBytesField payload = BufferCodecSupport.readIntBytesField(buffer, topic.nextOffset(), limit);
        return new EventView(buffer, topic, payload);
    }

    public record EventView(
            DirectBuffer buffer,
            BufferCodecSupport.ShortStringField topicField,
            BufferCodecSupport.IntBytesField payloadField
    ) {
        public String topic() {
            return topicField.decode(buffer);
        }

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
