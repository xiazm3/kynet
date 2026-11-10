package cn.itsub.kynet.common.codec;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.MessageLite;
import com.google.protobuf.StringValue;

public final class BroadcastPayloadCodec {
    private BroadcastPayloadCodec() {
    }

    public static byte[] encode(Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof byte[] bytes) {
            return bytes;
        }
        if (payload instanceof MessageLite messageLite) {
            return messageLite.toByteArray();
        }
        if (payload instanceof String text) {
            return StringValue.of(text).toByteArray();
        }
        if (payload instanceof Integer number) {
            return Int32Value.of(number).toByteArray();
        }
        if (payload instanceof Short number) {
            return Int32Value.of(number.intValue()).toByteArray();
        }
        if (payload instanceof Byte number) {
            return Int32Value.of(number.intValue()).toByteArray();
        }
        if (payload instanceof Long number) {
            return Int64Value.of(number).toByteArray();
        }
        if (payload instanceof Boolean bool) {
            return BoolValue.of(bool).toByteArray();
        }
        if (payload instanceof Float number) {
            return FloatValue.of(number).toByteArray();
        }
        if (payload instanceof Double number) {
            return DoubleValue.of(number).toByteArray();
        }
        if (payload.getClass().isAnnotationPresent(ProtobufClass.class)) {
            return ProtobufPayloadCodec.encode(payload);
        }
        throw new IllegalArgumentException("Unsupported broadcast payload type: " + payload.getClass().getName());
    }
}
