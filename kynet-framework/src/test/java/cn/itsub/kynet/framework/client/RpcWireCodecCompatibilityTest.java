package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.codec.RpcWireCodec;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcWireCodecCompatibilityTest {
    @Test
    void decodesIdempotentFlagAppendedAfterBody() {
        UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
        int length = RpcWireCodec.encodeRequest(
                payload, 0, 1L, 2L, 3,
                null, 1_000L, false,
                null, 0, 0, true);

        assertTrue(RpcWireCodec.decodeRequestView(payload, 0, length).idempotent());
    }

    @Test
    void treatsLegacyPayloadWithoutIdempotentFlagAsNonIdempotent() {
        UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
        int newLength = RpcWireCodec.encodeRequest(
                payload, 0, 1L, 2L, 3,
                null, 1_000L, false,
                null, 0, 0);

        assertFalse(RpcWireCodec.decodeRequestView(payload, 0, newLength - 1).idempotent());
    }
}
