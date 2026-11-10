package cn.itsub.kynet.common.codec;

import cn.itsub.kynet.common.protocol.MessageType;
import cn.itsub.kynet.common.protocol.WireMessage;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public final class FrameCodec {

    public static final int OFFSET_MESSAGE_TYPE = 0;
    public static final int OFFSET_ACTION = 1;
    public static final int OFFSET_SOURCE_INSTANCE_ID = 5;
    public static final int OFFSET_TARGET_INSTANCE_ID = 9;
    public static final int OFFSET_TOPIC_HASH = 13;
    public static final int FIXED_HEADER_LENGTH = 17;

    private static final MessageType[] MESSAGE_TYPES = MessageType.values();

    private FrameCodec() {
    }

    public static int encode(WireMessage msg, MutableDirectBuffer buffer, int offset) {
        return encode(
                msg.getMessageType(),
                msg.getAction(),
                msg.getSourceInstanceId(),
                msg.getTargetInstanceId(),
                msg.getTopicHash(),
                msg.getRequestId(),
                msg.getTopic(),
                msg.getTraceId(),
                msg.getPayloadType(),
                msg.getPayload(),
                buffer,
                offset
        );
    }

    public static int encode(MessageType messageType,
                             int action,
                             int sourceInstanceId,
                             int targetInstanceId,
                             int topicHash,
                             long requestId,
                             String topic,
                             String traceId,
                             String payloadType,
                             byte[] payload,
                             MutableDirectBuffer buffer,
                             int offset) {
        int pos = offset;
        buffer.putByte(pos + OFFSET_MESSAGE_TYPE, (byte) messageType.ordinal());
        buffer.putInt(pos + OFFSET_ACTION, action);
        buffer.putInt(pos + OFFSET_SOURCE_INSTANCE_ID, sourceInstanceId);
        buffer.putInt(pos + OFFSET_TARGET_INSTANCE_ID, targetInstanceId);
        buffer.putInt(pos + OFFSET_TOPIC_HASH, topicHash);
        pos += FIXED_HEADER_LENGTH;

        buffer.putLong(pos, requestId);
        pos += 8;

        pos = BufferCodecSupport.writeShortString(buffer, pos, topic);
        pos = BufferCodecSupport.writeShortString(buffer, pos, traceId);
        pos = BufferCodecSupport.writeShortString(buffer, pos, payloadType);
        if (payload != null && payload.length > 0) {
            buffer.putBytes(pos, payload);
            pos += payload.length;
        }
        return pos - offset;
    }

    public static int encode(MessageType messageType,
                             int action,
                             int sourceInstanceId,
                             int targetInstanceId,
                             int topicHash,
                             long requestId,
                             String topic,
                             String traceId,
                             String payloadType,
                             DirectBuffer payloadBuffer,
                             int payloadOffset,
                             int payloadLength,
                             MutableDirectBuffer buffer,
                             int offset) {
        int pos = offset;
        buffer.putByte(pos + OFFSET_MESSAGE_TYPE, (byte) messageType.ordinal());
        buffer.putInt(pos + OFFSET_ACTION, action);
        buffer.putInt(pos + OFFSET_SOURCE_INSTANCE_ID, sourceInstanceId);
        buffer.putInt(pos + OFFSET_TARGET_INSTANCE_ID, targetInstanceId);
        buffer.putInt(pos + OFFSET_TOPIC_HASH, topicHash);
        pos += FIXED_HEADER_LENGTH;

        buffer.putLong(pos, requestId);
        pos += 8;

        pos = BufferCodecSupport.writeShortString(buffer, pos, topic);
        pos = BufferCodecSupport.writeShortString(buffer, pos, traceId);
        pos = BufferCodecSupport.writeShortString(buffer, pos, payloadType);
        if (payloadBuffer != null && payloadLength > 0) {
            buffer.putBytes(pos, payloadBuffer, payloadOffset, payloadLength);
            pos += payloadLength;
        }
        return pos - offset;
    }

    public static FrameHeader readHeader(DirectBuffer buffer, int offset) {
        return new FrameHeader(
                buffer.getByte(offset + OFFSET_MESSAGE_TYPE) & 0xFF,
                buffer.getInt(offset + OFFSET_ACTION),
                buffer.getInt(offset + OFFSET_SOURCE_INSTANCE_ID),
                buffer.getInt(offset + OFFSET_TARGET_INSTANCE_ID),
                buffer.getInt(offset + OFFSET_TOPIC_HASH)
        );
    }

    public static int readMessageTypeOrdinal(DirectBuffer buffer, int offset) {
        return buffer.getByte(offset + OFFSET_MESSAGE_TYPE) & 0xFF;
    }

    public static int readAction(DirectBuffer buffer, int offset) {
        return buffer.getInt(offset + OFFSET_ACTION);
    }

    public static int readSourceInstanceId(DirectBuffer buffer, int offset) {
        return buffer.getInt(offset + OFFSET_SOURCE_INSTANCE_ID);
    }

    public static int readTargetInstanceId(DirectBuffer buffer, int offset) {
        return buffer.getInt(offset + OFFSET_TARGET_INSTANCE_ID);
    }

    public static int readTopicHash(DirectBuffer buffer, int offset) {
        return buffer.getInt(offset + OFFSET_TOPIC_HASH);
    }

    public static long readRequestId(DirectBuffer buffer, int offset) {
        return buffer.getLong(offset + FIXED_HEADER_LENGTH);
    }

    /**
     * Allocation-free counterpart of {@link #decodeView} for hot receive paths: skips the
     * fixed header and the three length-prefixed header strings to locate the payload.
     */
    public static int payloadOffset(DirectBuffer buffer, int offset, int length) {
        int limit = offset + length;
        int pos = offset + FIXED_HEADER_LENGTH + Long.BYTES;
        for (int i = 0; i < 3; i++) {
            if (pos + Short.BYTES > limit) {
                return limit;
            }
            int stringLength = buffer.getShort(pos) & 0xFFFF;
            pos = Math.min(limit, pos + Short.BYTES + stringLength);
        }
        return pos;
    }

    public static DecodedFrame decodeView(DirectBuffer buffer, int offset, int length) {
        int ordinal = buffer.getByte(offset + OFFSET_MESSAGE_TYPE) & 0xFF;
        MessageType messageType = ordinal < MESSAGE_TYPES.length ? MESSAGE_TYPES[ordinal] : null;
        int action = buffer.getInt(offset + OFFSET_ACTION);
        int sourceInstanceId = buffer.getInt(offset + OFFSET_SOURCE_INSTANCE_ID);
        int targetInstanceId = buffer.getInt(offset + OFFSET_TARGET_INSTANCE_ID);
        int topicHash = buffer.getInt(offset + OFFSET_TOPIC_HASH);

        int pos = offset + FIXED_HEADER_LENGTH;
        int end = offset + length;
        long requestId = buffer.getLong(pos);
        pos += 8;

        BufferCodecSupport.ShortStringField topicResult = BufferCodecSupport.readShortStringField(buffer, pos, end);
        pos = topicResult.nextOffset();

        BufferCodecSupport.ShortStringField traceIdResult = BufferCodecSupport.readShortStringField(buffer, pos, end);
        pos = traceIdResult.nextOffset();

        BufferCodecSupport.ShortStringField payloadTypeResult = BufferCodecSupport.readShortStringField(buffer, pos, end);
        pos = payloadTypeResult.nextOffset();

        return new DecodedFrame(
                messageType,
                requestId,
                action,
                sourceInstanceId,
                targetInstanceId,
                topicHash,
                buffer,
                topicResult.dataOffset(),
                topicResult.length(),
                traceIdResult.dataOffset(),
                traceIdResult.length(),
                payloadTypeResult.dataOffset(),
                payloadTypeResult.length(),
                pos,
                Math.max(0, end - pos)
        );
    }

    public static WireMessage decode(DirectBuffer buffer, int offset, int length) {
        DecodedFrame frame = decodeView(buffer, offset, length);
        WireMessage msg = new WireMessage();
        msg.setMessageType(frame.messageType());
        msg.setAction(frame.action());
        msg.setSourceInstanceId(frame.sourceInstanceId());
        msg.setTargetInstanceId(frame.targetInstanceId());
        msg.setRequestId(frame.requestId());
        msg.setTopic(frame.topic());
        msg.setTraceId(frame.traceId());
        msg.setPayloadType(frame.payloadType());

        if (frame.payloadLength() > 0) {
            byte[] payload = new byte[frame.payloadLength()];
            frame.payloadBuffer().getBytes(frame.payloadOffset(), payload);
            msg.setPayload(payload);
        }
        return msg;
    }

    public record DecodedFrame(
            MessageType messageType,
            long requestId,
            int action,
            int sourceInstanceId,
            int targetInstanceId,
            int topicHash,
            DirectBuffer payloadBuffer,
            int topicOffset,
            int topicLength,
            int traceIdOffset,
            int traceIdLength,
            int payloadTypeOffset,
            int payloadTypeLength,
            int payloadOffset,
            int payloadLength
    ) {
        public String topic() {
            return BufferCodecSupport.readString(payloadBuffer, topicOffset, topicLength);
        }

        public String traceId() {
            return BufferCodecSupport.readString(payloadBuffer, traceIdOffset, traceIdLength);
        }

        public String payloadType() {
            return BufferCodecSupport.readString(payloadBuffer, payloadTypeOffset, payloadTypeLength);
        }
    }
}
