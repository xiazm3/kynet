package cn.itsub.kynet.common.codec;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class GatewayKickWireCodecTest {

    @Test
    void roundTripsAllFields() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
        int length = GatewayKickWireCodec.encode(buffer, 0, 7L, 10001L, 9L, "账号在其他设备登录");

        GatewayKickWireCodec.GatewayKickView view = GatewayKickWireCodec.decodeView(buffer, 0, length);
        assertEquals(7L, view.sessionId());
        assertEquals(10001L, view.userId());
        assertEquals(9L, view.exceptSessionId());
        assertEquals("账号在其他设备登录", view.reason());
    }

    @Test
    void nullReasonDecodesAsNull() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
        int length = GatewayKickWireCodec.encode(buffer, 0, 0L, 42L, 0L, null);

        GatewayKickWireCodec.GatewayKickView view = GatewayKickWireCodec.decodeView(buffer, 0, length);
        assertNull(view.reason());
    }

    @Test
    void truncatedPayloadDegradesToZeroesWithoutThrowing() {
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
        GatewayKickWireCodec.encode(buffer, 0, 7L, 10001L, 9L, "kick");
        GatewayKickWireCodec.GatewayKickView view = GatewayKickWireCodec.decodeView(buffer, 0, 10);

        assertEquals(7L, view.sessionId());
        assertEquals(0L, view.userId());
        assertEquals(0L, view.exceptSessionId());
        assertNull(view.reason());
    }
}
