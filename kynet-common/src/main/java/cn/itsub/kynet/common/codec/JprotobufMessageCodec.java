package cn.itsub.kynet.common.codec;

import cn.itsub.kynet.common.protocol.WireMessage;
import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;

public class JprotobufMessageCodec implements MessageCodec {
    private static final Codec<WireMessage> WIRE_CODEC = ProtobufProxy.create(WireMessage.class);

    @Override
    public byte[] encode(WireMessage message) {
        try {
            return WIRE_CODEC.encode(message);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode wire message", e);
        }
    }

    @Override
    public WireMessage decode(byte[] encoded) {
        try {
            return WIRE_CODEC.decode(encoded);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to decode wire message", e);
        }
    }
}
