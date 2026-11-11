package cn.itsub.kynet.common.codec;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ProtobufPayloadCodec {
    private static final Map<Class<?>, Codec<?>> CODEC_CACHE = new ConcurrentHashMap<>();

    private ProtobufPayloadCodec() {
    }

    public static byte[] encode(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        if (value instanceof List<?> list) {
            return encode(ListValues.of(list));
        }
        if (value instanceof Integer number) {
            return encode(new PrimitiveValue().setIntValue(number).setValueKind(PrimitiveValue.Kind.INT));
        }
        if (value instanceof Long number) {
            return encode(new PrimitiveValue().setLongValue(number).setValueKind(PrimitiveValue.Kind.LONG));
        }
        if (value instanceof Boolean bool) {
            return encode(new PrimitiveValue().setBoolValue(bool).setValueKind(PrimitiveValue.Kind.BOOL));
        }
        if (value instanceof Double number) {
            return encode(new PrimitiveValue().setDoubleValue(number).setValueKind(PrimitiveValue.Kind.DOUBLE));
        }
        if (value instanceof Float number) {
            return encode(new PrimitiveValue().setFloatValue(number).setValueKind(PrimitiveValue.Kind.FLOAT));
        }
        Codec<Object> codec = resolveCodec((Class<Object>) value.getClass());
        try {
            return codec.encode(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode payload type " + value.getClass().getName(), e);
        }
    }

    public static <T> T decode(byte[] bytes, Class<T> targetType) {
        if (bytes == null) {
            return null;
        }
        return decodeInternal(new UnsafeBuffer(bytes), 0, bytes.length, targetType);
    }

    public static <T> T decode(DirectBuffer buffer, int offset, int length, Class<T> targetType) {
        if (buffer == null || length <= 0) {
            return null;
        }
        return decodeInternal(buffer, offset, length, targetType);
    }

    private static <T> T decodeInternal(DirectBuffer buffer, int offset, int length, Class<T> targetType) {
        if (byte[].class.equals(targetType)) {
            byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            return targetType.cast(bytes);
        }
        if (String.class.equals(targetType)) {
            return targetType.cast(readUtf8(buffer, offset, length));
        }
        if (Integer.class.equals(targetType) || int.class.equals(targetType)) {
            PrimitiveValue value = decodeInternal(buffer, offset, length, PrimitiveValue.class);
            return (T) value.getIntValue();
        }
        if (Long.class.equals(targetType) || long.class.equals(targetType)) {
            PrimitiveValue value = decodeInternal(buffer, offset, length, PrimitiveValue.class);
            return (T) value.getLongValue();
        }
        if (Boolean.class.equals(targetType) || boolean.class.equals(targetType)) {
            PrimitiveValue value = decodeInternal(buffer, offset, length, PrimitiveValue.class);
            return (T) value.getBoolValue();
        }
        if (Double.class.equals(targetType) || double.class.equals(targetType)) {
            PrimitiveValue value = decodeInternal(buffer, offset, length, PrimitiveValue.class);
            return (T) value.getDoubleValue();
        }
        if (Float.class.equals(targetType) || float.class.equals(targetType)) {
            PrimitiveValue value = decodeInternal(buffer, offset, length, PrimitiveValue.class);
            return (T) value.getFloatValue();
        }
        Codec<T> codec = resolveCodec(targetType);
        try {
            return codec.readFrom(newInputSource(buffer, offset, length).input());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to decode payload type " + targetType.getName(), e);
        }
    }

    public static Object decode(byte[] bytes, String className) {
        if (className == null || className.isBlank() || bytes == null) {
            return null;
        }
        try {
            return decode(bytes, (Class<Object>) Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unknown payload class " + className, e);
        }
    }

    public static Object decode(DirectBuffer buffer, int offset, int length, String className) {
        if (className == null || className.isBlank() || buffer == null || length <= 0) {
            return null;
        }
        try {
            return decode(buffer, offset, length, (Class<Object>) Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unknown payload class " + className, e);
        }
    }

    public static RpcRequestView decodeRpcRequestView(DirectBuffer buffer, int offset, int length) {
        if (buffer == null || length <= 0) {
            return new RpcRequestView(0, null, 0L, 0L, 0L, 0, 0, false, null, buffer, offset, 0);
        }

        InputSource source = newInputSource(buffer, offset, length);
        CodedInputStream input = source.input();
        int action = 0;
        String bodyType = null;
        long sessionId = 0L;
        long userId = 0L;
        long timeoutMs = 0L;
        int targetServiceId = 0;
        int targetInstanceId = 0;
        boolean oneWay = false;
        byte[] attachmentData = null;
        int bodyOffset = source.baseOffset();
        int bodyLength = 0;

        try {
            while (!input.isAtEnd()) {
                int tag = input.readTag();
                if (tag == 0) {
                    break;
                }
                switch (WireFormat.getTagFieldNumber(tag)) {
                    case 1 -> action = input.readInt32();
                    case 2 -> bodyType = input.readStringRequireUtf8();
                    case 3 -> {
                        if (WireFormat.getTagWireType(tag) != WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                            input.skipField(tag);
                            continue;
                        }
                        bodyLength = input.readRawVarint32();
                        bodyOffset = source.baseOffset() + input.getTotalBytesRead();
                        input.skipRawBytes(bodyLength);
                    }
                    case 4 -> sessionId = input.readInt64();
                    case 5 -> userId = input.readInt64();
                    case 6 -> timeoutMs = input.readInt64();
                    case 7 -> targetServiceId = input.readInt32();
                    case 8 -> targetInstanceId = input.readInt32();
                    case 9 -> oneWay = input.readBool();
                    case 10 -> attachmentData = input.readByteArray();
                    default -> input.skipField(tag);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode RpcRequest view", e);
        }

        return new RpcRequestView(
                action,
                bodyType,
                sessionId,
                userId,
                timeoutMs,
                targetServiceId,
                targetInstanceId,
                oneWay,
                attachmentData,
                source.buffer(),
                bodyOffset,
                bodyLength
        );
    }

    private static <T> Codec<T> resolveCodec(Class<T> type) {
        Codec<?> codec = CODEC_CACHE.computeIfAbsent(type, key -> {
            if (!key.isAnnotationPresent(ProtobufClass.class)) {
                throw new IllegalStateException("Type " + key.getName() + " must declare @ProtobufClass for jprotobuf serialization");
            }
            return ProtobufProxy.create(key);
        });
        return (Codec<T>) codec;
    }

    private static String readUtf8(DirectBuffer buffer, int offset, int length) {
        ByteBuffer byteBuffer = buffer.byteBuffer();
        if (byteBuffer != null) {
            int baseOffset = buffer.wrapAdjustment() + offset;
            ByteBuffer view = byteBuffer.duplicate();
            view.position(baseOffset);
            view.limit(baseOffset + length);
            return StandardCharsets.UTF_8.decode(view.slice()).toString();
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static InputSource newInputSource(DirectBuffer buffer, int offset, int length) {
        ByteBuffer byteBuffer = buffer.byteBuffer();
        if (byteBuffer != null) {
            int baseOffset = buffer.wrapAdjustment() + offset;
            ByteBuffer view = byteBuffer.duplicate();
            view.position(baseOffset);
            view.limit(baseOffset + length);
            CodedInputStream input = CodedInputStream.newInstance(view.slice());
            input.enableAliasing(true);
            return new InputSource(input, buffer, offset);
        }

        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return new InputSource(CodedInputStream.newInstance(bytes), new UnsafeBuffer(bytes), 0);
    }

    private record InputSource(CodedInputStream input, DirectBuffer buffer, int baseOffset) {
    }

    public record RpcRequestView(
            int action,
            String bodyType,
            long sessionId,
            long userId,
            long timeoutMs,
            int targetServiceId,
            int targetInstanceId,
            boolean oneWay,
            byte[] attachmentData,
            DirectBuffer bodyBuffer,
            int bodyOffset,
            int bodyLength
    ) {
    }
}
