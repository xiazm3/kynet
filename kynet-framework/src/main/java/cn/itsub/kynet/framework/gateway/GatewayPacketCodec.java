package cn.itsub.kynet.framework.gateway;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.charset.StandardCharsets;

public final class GatewayPacketCodec {
    public static final int MAGIC = 0x4B594E54;
    public static final short VERSION = 1;
    public static final int DEFAULT_MAX_PAYLOAD_LENGTH = 16 * 1024 * 1024;
    public static final int DEFAULT_MAX_STRING_LENGTH = 64 * 1024;
    private static final int FIXED_HEADER_LENGTH = Integer.BYTES + Short.BYTES + Byte.BYTES + Byte.BYTES
            + Long.BYTES + Integer.BYTES + Integer.BYTES
            + Long.BYTES + Long.BYTES + Long.BYTES;

    private GatewayPacketCodec() {
    }

    public static void encode(ByteBuf out, GatewayPacket packet) {
        out.writeInt(MAGIC);
        out.writeShort(VERSION);
        out.writeByte(packet.type().ordinal());
        out.writeBoolean(packet.oneWay());
        out.writeLong(packet.requestId());
        out.writeInt(packet.cmd());
        out.writeInt(packet.act());
        out.writeLong(packet.timeoutMillis());
        out.writeLong(packet.sessionId());
        out.writeLong(packet.userId());
        writeString(out, packet.errorCode());
        writeString(out, packet.errorMessage());
        writeBytes(out, packet.payload());
    }

    public static int encodedLength(GatewayPacket packet) {
        long length = FIXED_HEADER_LENGTH;
        length += encodedStringLength(packet.errorCode());
        length += encodedStringLength(packet.errorMessage());
        length += encodedBytesLength(packet.payload());
        return Math.toIntExact(length);
    }

    public static GatewayPacket decode(ByteBuf in) {
        return decode(in, DEFAULT_MAX_PAYLOAD_LENGTH, DEFAULT_MAX_STRING_LENGTH);
    }

    public static GatewayPacket decode(ByteBuf in, int maxPayloadLength, int maxStringLength) {
        if (maxPayloadLength < 0 || maxStringLength < 0) {
            throw new IllegalArgumentException("gateway packet length limits must not be negative");
        }
        requireReadable(in, FIXED_HEADER_LENGTH, "gateway packet header");
        int magic = in.readInt();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Unsupported gateway packet magic: " + magic);
        }
        short version = in.readShort();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported gateway packet version: " + version);
        }
        int typeOrdinal = in.readUnsignedByte();
        GatewayPacketType[] packetTypes = GatewayPacketType.values();
        if (typeOrdinal >= packetTypes.length) {
            throw new IllegalArgumentException("Unknown gateway packet type ordinal: " + typeOrdinal);
        }
        boolean oneWay = in.readBoolean();
        long requestId = in.readLong();
        int cmd = in.readInt();
        int act = in.readInt();
        long timeoutMillis = in.readLong();
        long sessionId = in.readLong();
        long userId = in.readLong();
        String errorCode = readString(in, maxStringLength, "errorCode");
        String errorMessage = readString(in, maxStringLength, "errorMessage");
        byte[] payload = readBytes(in, maxPayloadLength);
        return new GatewayPacket(packetTypes[typeOrdinal], requestId, cmd, act, oneWay, timeoutMillis,
                sessionId, userId, errorCode, errorMessage, false, null, payload);
    }

    private static void writeString(ByteBuf out, String value) {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.writeBytes(bytes);
    }

    private static int encodedStringLength(String value) {
        return Integer.BYTES + (value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length);
    }

    private static String readString(ByteBuf in, int maxLength, String fieldName) {
        requireReadable(in, Integer.BYTES, fieldName + " length");
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < -1 || length > maxLength) {
            throw new IllegalArgumentException("Invalid gateway " + fieldName + " length: " + length);
        }
        requireReadable(in, length, fieldName);
        return in.readCharSequence(length, StandardCharsets.UTF_8).toString();
    }

    private static void writeBytes(ByteBuf out, byte[] value) {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(value.length);
        out.writeBytes(value);
    }

    private static int encodedBytesLength(byte[] value) {
        return Integer.BYTES + (value == null ? 0 : value.length);
    }

    private static byte[] readBytes(ByteBuf in, int maxLength) {
        requireReadable(in, Integer.BYTES, "payload length");
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < -1 || length > maxLength) {
            throw new IllegalArgumentException("Invalid gateway payload length: " + length);
        }
        requireReadable(in, length, "payload");
        byte[] bytes = ByteBufUtil.getBytes(in, in.readerIndex(), length, false);
        in.skipBytes(length);
        return bytes;
    }

    private static void requireReadable(ByteBuf in, int length, String fieldName) {
        if (length < 0 || in.readableBytes() < length) {
            throw new IllegalArgumentException("Truncated gateway " + fieldName
                    + ": required=" + length + ", readable=" + in.readableBytes());
        }
    }
}
