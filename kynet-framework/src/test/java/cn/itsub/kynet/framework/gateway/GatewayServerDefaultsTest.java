package cn.itsub.kynet.framework.gateway;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayServerDefaultsTest {

    @Test
    void usesProductionReadyDefaultsWithoutApplicationOverrides() {
        GatewayServer server = new GatewayServer(null, "tcp://127.0.0.1:9000");

        assertAll(
                () -> assertEquals(GatewayServer.DEFAULT_MAX_QPS_PER_PLAYER, server.getMaxQpsPerPlayer()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_QPS_PER_IP, server.getMaxQpsPerIp()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_QPS_PER_USER, server.getMaxQpsPerUser()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_QPS_PER_ACTION, server.getMaxQpsPerAction()),
                () -> assertEquals(GatewayServer.DEFAULT_SESSION_IDLE_TIMEOUT_SECONDS,
                        server.getSessionIdleTimeoutSeconds()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_CONNECTIONS, server.getMaxConnections()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_CONNECTIONS_PER_IP,
                        server.getMaxConnectionsPerIp()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_CONNECTION_ATTEMPTS_PER_IP_PER_SECOND,
                        server.getMaxConnectionAttemptsPerIpPerSecond()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_FRAME_LENGTH, server.getMaxFrameLength()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_PAYLOAD_LENGTH, server.getMaxPayloadLength()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_STRING_LENGTH, server.getMaxStringLength()),
                () -> assertEquals(GatewayServer.DEFAULT_WRITE_BUFFER_LOW_WATER_MARK,
                        server.getWriteBufferLowWaterMark()),
                () -> assertEquals(GatewayServer.DEFAULT_WRITE_BUFFER_HIGH_WATER_MARK,
                        server.getWriteBufferHighWaterMark()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_PENDING_WRITE_BYTES,
                        server.getMaxPendingWriteBytes()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_PENDING_WRITE_MESSAGES,
                        server.getMaxPendingWriteMessages()),
                () -> assertEquals(GatewayServer.DEFAULT_MAX_UNWRITABLE_DURATION_MILLIS,
                        server.getMaxUnwritableDurationMillis()),
                () -> assertEquals(GatewayServer.DEFAULT_SLOW_CLIENT_POLICY, server.getSlowClientPolicy()),
                () -> assertTrue(server.isWebSocketOriginAllowed("https://game.example.com"))
        );
    }

    @Test
    void zeroStillExplicitlyDisablesCapacityAndRateLimits() {
        GatewayServer server = new GatewayServer(null, "tcp://127.0.0.1:9000");

        server.setMaxQpsPerPlayer(0);
        server.setMaxQpsPerIp(0);
        server.setMaxQpsPerUser(0);
        server.setMaxQpsPerAction(0);
        server.setMaxConnections(0);
        server.setMaxConnectionsPerIp(0);
        server.setMaxConnectionAttemptsPerIpPerSecond(0);

        assertAll(
                () -> assertEquals(0, server.getMaxQpsPerPlayer()),
                () -> assertEquals(0, server.getMaxQpsPerIp()),
                () -> assertEquals(0, server.getMaxQpsPerUser()),
                () -> assertEquals(0, server.getMaxQpsPerAction()),
                () -> assertEquals(0, server.getMaxConnections()),
                () -> assertEquals(0, server.getMaxConnectionsPerIp()),
                () -> assertEquals(0, server.getMaxConnectionAttemptsPerIpPerSecond())
        );
    }
}
