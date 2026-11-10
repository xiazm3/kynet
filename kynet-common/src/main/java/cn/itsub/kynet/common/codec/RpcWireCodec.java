package cn.itsub.kynet.common.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public final class RpcWireCodec {
    private RpcWireCodec() {
    }

    public static int encodeRequest(MutableDirectBuffer buffer, int offset,
                                    long sessionId, long userId, int gatewayInstanceId,
                                    byte[] attachmentData,
                                    long timeoutMs, boolean oneWay,
                                    DirectBuffer bodyBuffer, int bodyOffset, int bodyLength) {
        return encodeRequest(buffer, offset, sessionId, userId, gatewayInstanceId,
                attachmentData, timeoutMs, oneWay,
                bodyBuffer, bodyOffset, bodyLength, false);
    }

    public static int encodeRequest(MutableDirectBuffer buffer, int offset,
                                    long sessionId, long userId, int gatewayInstanceId,
                                    byte[] attachmentData,
                                    long timeoutMs, boolean oneWay,
                                    DirectBuffer bodyBuffer, int bodyOffset, int bodyLength,
                                    boolean idempotent) {
        int pos = offset;
        buffer.putLong(pos, sessionId);
        pos += 8;
        buffer.putLong(pos, userId);
        pos += 8;
        buffer.putInt(pos, gatewayInstanceId);
        pos += 4;
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, attachmentData);
        buffer.putLong(pos, timeoutMs);
        pos += 8;
        buffer.putByte(pos, (byte) (oneWay ? 1 : 0));
        pos += 1;
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, bodyBuffer, bodyOffset, bodyLength);
        buffer.putByte(pos, (byte) (idempotent ? 1 : 0));
        pos += 1;
        return pos - offset;
    }

    public static int encodeResponse(MutableDirectBuffer buffer, int offset,
                                     boolean success, String errorCode, String errorMessage,
                                     boolean attachmentUpdated,
                                     byte[] attachmentData,
                                     DirectBuffer bodyBuffer, int bodyOffset, int bodyLength) {
        int pos = offset;
        buffer.putByte(pos, (byte) (success ? 1 : 0));
        pos += 1;
        pos = BufferCodecSupport.writeShortString(buffer, pos, errorCode);
        pos = BufferCodecSupport.writeShortString(buffer, pos, errorMessage);
        buffer.putByte(pos, (byte) (attachmentUpdated ? 1 : 0));
        pos += 1;
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, attachmentData);
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, bodyBuffer, bodyOffset, bodyLength);
        return pos - offset;
    }

    public static int encodeResponse(MutableDirectBuffer buffer, int offset,
                                     boolean success, String errorCode, String errorMessage,
                                     boolean attachmentUpdated,
                                     byte[] attachmentData,
                                     byte[] body) {
        int pos = offset;
        buffer.putByte(pos, (byte) (success ? 1 : 0));
        pos += 1;
        pos = BufferCodecSupport.writeShortString(buffer, pos, errorCode);
        pos = BufferCodecSupport.writeShortString(buffer, pos, errorMessage);
        buffer.putByte(pos, (byte) (attachmentUpdated ? 1 : 0));
        pos += 1;
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, attachmentData);
        pos = BufferCodecSupport.writeIntBytes(buffer, pos, body);
        return pos - offset;
    }

    public static RpcRequestView decodeRequestView(DirectBuffer buffer, int offset, int length) {
        int limit = offset + length;
        long sessionId = offset + 8 <= limit ? buffer.getLong(offset) : 0L;
        int pos = offset + 8;
        long userId = pos + 8 <= limit ? buffer.getLong(pos) : 0L;
        pos += 8;
        int gatewayInstanceId = pos + 4 <= limit ? buffer.getInt(pos) : 0;
        pos += 4;
        BufferCodecSupport.IntBytesField attachment = BufferCodecSupport.readIntBytesField(buffer, pos, limit);
        pos = attachment.nextOffset();
        long timeoutMs = pos + 8 <= limit ? buffer.getLong(pos) : 0L;
        pos += 8;
        boolean oneWay = pos < limit && buffer.getByte(pos) != 0;
        pos += 1;
        BufferCodecSupport.IntBytesField body = BufferCodecSupport.readIntBytesField(buffer, pos, limit);
        pos = body.nextOffset();
        boolean idempotent = pos < limit && buffer.getByte(pos) != 0;
        return new RpcRequestView(buffer, sessionId, userId, gatewayInstanceId, attachment,
                timeoutMs, oneWay, idempotent, body);
    }

    public static RpcResponseView decodeResponseView(DirectBuffer buffer, int offset, int length) {
        int limit = offset + length;
        boolean success = offset < limit && buffer.getByte(offset) != 0;
        int pos = offset + 1;
        BufferCodecSupport.ShortStringField errorCode = BufferCodecSupport.readShortStringField(buffer, pos, limit);
        BufferCodecSupport.ShortStringField errorMessage = BufferCodecSupport.readShortStringField(buffer, errorCode.nextOffset(), limit);
        pos = errorMessage.nextOffset();
        boolean attachmentUpdated = pos < limit && buffer.getByte(pos) != 0;
        pos += 1;
        BufferCodecSupport.IntBytesField attachment = BufferCodecSupport.readIntBytesField(buffer, pos, limit);
        BufferCodecSupport.IntBytesField body = BufferCodecSupport.readIntBytesField(buffer, attachment.nextOffset(), limit);
        return new RpcResponseView(buffer, success, errorCode, errorMessage, attachmentUpdated, attachment, body);
    }

    public record RpcRequestView(
            DirectBuffer buffer,
            long sessionId,
            long userId,
            int gatewayInstanceId,
            BufferCodecSupport.IntBytesField attachmentField,
            long timeoutMs,
            boolean oneWay,
            boolean idempotent,
            BufferCodecSupport.IntBytesField bodyField
    ) {
        public DirectBuffer bodyBuffer() {
            return buffer;
        }

        public byte[] copyAttachmentData() {
            return attachmentField.copy(buffer);
        }

        public DirectBuffer attachmentBuffer() {
            return buffer;
        }

        public int attachmentOffset() {
            return attachmentField.dataOffset();
        }

        public int attachmentLength() {
            return attachmentField.length();
        }

        public int bodyOffset() {
            return bodyField.dataOffset();
        }

        public int bodyLength() {
            return bodyField.length();
        }
    }

    public record RpcResponseView(
            DirectBuffer buffer,
            boolean success,
            BufferCodecSupport.ShortStringField errorCodeField,
            BufferCodecSupport.ShortStringField errorMessageField,
            boolean attachmentUpdated,
            BufferCodecSupport.IntBytesField attachmentField,
            BufferCodecSupport.IntBytesField bodyField
    ) {
        public String errorCode() {
            return errorCodeField.decode(buffer);
        }

        public String errorMessage() {
            return errorMessageField.decode(buffer);
        }

        public DirectBuffer bodyBuffer() {
            return buffer;
        }

        public byte[] copyAttachmentData() {
            return attachmentField.copy(buffer);
        }

        public int bodyOffset() {
            return bodyField.dataOffset();
        }

        public int bodyLength() {
            return bodyField.length();
        }

        public byte[] copyBody() {
            return bodyField.copy(buffer);
        }
    }
}
