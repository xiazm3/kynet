package cn.itsub.kynet.framework.gateway;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayPacketCodecSecurityTest {

    @Test
    void secureEndpointSchemesAreExplicit() {
        GatewayEndpoint tcp = GatewayEndpoint.parse("tcp://127.0.0.1:30001");
        GatewayEndpoint tcps = GatewayEndpoint.parse("tcps://127.0.0.1:30002");
        GatewayEndpoint ws = GatewayEndpoint.parse("ws://127.0.0.1:30003/ws");
        GatewayEndpoint wss = GatewayEndpoint.parse("wss://127.0.0.1:30004/ws");

        assertFalse(tcp.secure());
        assertTrue(tcps.secure());
        assertEquals(GatewayProtocol.TCP, tcps.protocol());
        assertFalse(ws.secure());
        assertTrue(wss.secure());
        assertEquals(GatewayProtocol.WS, wss.protocol());
    }

    @Test
    void tlsConfigDoesNotExposePrivateKeyPassword() {
        GatewayTlsConfig config = GatewayTlsConfig.of("certificate.pem", "private-key.pem", "secret-value");

        assertFalse(config.toString().contains("secret-value"));
    }

    @Test
    void decodeRejectsPayloadOverConfiguredLimit() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            GatewayPacketCodec.encode(buffer, GatewayPacket.request(
                    1L, 1, 1, false, 1_000L, 0L, 0L,
                    null, new byte[32]));

            assertThrows(IllegalArgumentException.class,
                    () -> GatewayPacketCodec.decode(buffer, 16, 128));
        } finally {
            buffer.release();
        }
    }

    @Test
    void decodeRejectsStringOverConfiguredLimit() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            GatewayPacketCodec.encode(buffer, GatewayPacket.error(
                    1L, 1, 1, 0L, 0L,
                    "ERROR_CODE_TOO_LONG", "message", false, null));

            assertThrows(IllegalArgumentException.class,
                    () -> GatewayPacketCodec.decode(buffer, 128, 8));
        } finally {
            buffer.release();
        }
    }

    @Test
    void decodeRejectsTruncatedPacket() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            buffer.writeInt(GatewayPacketCodec.MAGIC);
            assertThrows(IllegalArgumentException.class, () -> GatewayPacketCodec.decode(buffer));
        } finally {
            buffer.release();
        }
    }

    @Test
    void encodedLengthMatchesWireCodec() {
        GatewayPacket packet = GatewayPacket.error(
                7L, 1, 2, 3L, 4L,
                "BAD_REQUEST", "错误消息", false, null);
        ByteBuf buffer = Unpooled.buffer();
        try {
            GatewayPacketCodec.encode(buffer, packet);
            assertEquals(buffer.readableBytes(), GatewayPacketCodec.encodedLength(packet));
        } finally {
            buffer.release();
        }
    }
}
