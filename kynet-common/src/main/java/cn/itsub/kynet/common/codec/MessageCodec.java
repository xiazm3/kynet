package cn.itsub.kynet.common.codec;

import cn.itsub.kynet.common.protocol.WireMessage;

public interface MessageCodec {
    byte[] encode(WireMessage message);

    WireMessage decode(byte[] encoded);
}
