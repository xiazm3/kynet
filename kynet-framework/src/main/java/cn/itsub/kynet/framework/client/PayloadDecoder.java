package cn.itsub.kynet.framework.client;

import org.agrona.DirectBuffer;

@FunctionalInterface
public interface PayloadDecoder<T> {
    T decode(DirectBuffer buffer, int offset, int length);
}
