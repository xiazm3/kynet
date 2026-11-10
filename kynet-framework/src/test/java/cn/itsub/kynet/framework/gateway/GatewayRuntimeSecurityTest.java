package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import cn.itsub.kynet.framework.user.UserSession;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayRuntimeSecurityTest {

    @Test
    void globalConnectionLimitReleasesCapacityOnDisconnect() {
        GatewayServer server = new GatewayServer(new RecordingServiceNode(), "tcp://127.0.0.1:9000");
        server.setMaxConnections(1);
        GatewayRuntime runtime = new GatewayRuntime(server);
        TestConnection first = connection(1L, "10.0.0.1");
        TestConnection second = connection(2L, "10.0.0.2");

        assertTrue(runtime.tryRegisterConnection(first));
        assertFalse(runtime.tryRegisterConnection(second));
        assertTrue(runtime.unregisterConnection(first));
        assertTrue(runtime.tryRegisterConnection(second));
        runtime.unregisterConnection(second);
        runtime.close();
    }

    @Test
    void perIpConnectionAndConnectionRateLimitsAreEnforced() {
        GatewayServer server = new GatewayServer(new RecordingServiceNode(), "tcp://127.0.0.1:9000");
        server.setMaxConnectionsPerIp(1);
        server.setMaxConnectionAttemptsPerIpPerSecond(2);
        GatewayRuntime runtime = new GatewayRuntime(server);
        TestConnection first = connection(11L, "10.0.0.1");
        TestConnection second = connection(12L, "10.0.0.1");
        TestConnection third = connection(13L, "10.0.0.1");

        assertTrue(runtime.tryRegisterConnection(first));
        assertFalse(runtime.tryRegisterConnection(second));
        runtime.unregisterConnection(first);
        assertFalse(runtime.tryRegisterConnection(third));
        runtime.close();
    }

    @Test
    void requestAuthenticatorCanRejectBeforeBrokerInvocation() {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        GatewayServer server = new GatewayServer(brokerClient, "tcp://127.0.0.1:9000");
        server.setMaxQpsPerPlayer(0);
        server.setRequestAuthenticator(context -> GatewayAuthenticationResult.deny(
                "TOKEN_INVALID", "token is invalid"));
        GatewayRuntime runtime = new GatewayRuntime(server);
        TestConnection connection = connection(21L, "10.0.0.1");

        runtime.handleIncoming(connection, request(1L));

        assertEquals(0, brokerClient.invocations.get());
        assertEquals("TOKEN_INVALID", connection.lastPacket().errorCode());
        runtime.close();
    }

    @Test
    void ipQpsLimitAppliesAcrossSessions() {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        GatewayServer server = new GatewayServer(brokerClient, "tcp://127.0.0.1:9000");
        server.setMaxQpsPerPlayer(0);
        server.setMaxQpsPerIp(1);
        GatewayRuntime runtime = new GatewayRuntime(server);
        TestConnection first = connection(31L, "10.0.0.1");
        TestConnection second = connection(32L, "10.0.0.1");

        runtime.handleIncoming(first, request(1L));
        runtime.handleIncoming(second, request(2L));

        assertEquals(1, brokerClient.invocations.get());
        assertEquals("RATE_LIMITED", second.lastPacket().errorCode());
        assertEquals("ip qps exceeded", second.lastPacket().errorMessage());
        runtime.close();
    }

    @Test
    void closeSlowClientPolicyClosesAndReleasesConnection() {
        GatewayServer server = new GatewayServer(new RecordingServiceNode(), "tcp://127.0.0.1:9000");
        server.setMaxConnections(1);
        server.setSlowClientPolicy(GatewaySlowClientPolicy.CLOSE);
        GatewayRuntime runtime = new GatewayRuntime(server);
        TestConnection slow = connection(41L, "10.0.0.1");
        TestConnection replacement = connection(42L, "10.0.0.2");

        assertTrue(runtime.tryRegisterConnection(slow));
        assertFalse(runtime.allowOutboundWrite(slow));
        assertFalse(slow.isOpen());
        assertEquals(1L, runtime.outboundBackpressureCount());
        assertTrue(runtime.tryRegisterConnection(replacement));
        runtime.unregisterConnection(replacement);
        runtime.close();
    }

    private static GatewayPacket request(long requestId) {
        return GatewayPacket.request(requestId, 1, 1, false, 1_000L,
                0L, 0L, null, new byte[0]);
    }

    private static TestConnection connection(long sessionId, String remoteIp) {
        return new TestConnection(new UserSession(sessionId, "tcp", 1, remoteIp + ":10000"), remoteIp);
    }

    private static final class RecordingServiceNode extends ServiceNode {
        private final AtomicInteger invocations = new AtomicInteger();

        private RecordingServiceNode() {
            super("127.0.0.1", 1, "GatewayRuntimeSecurityTest");
        }

        @Override
        public CompletableFuture<ActionResult> invokeAsync(int cmd, int act, Object payload, InvokeOptions options) {
            invocations.incrementAndGet();
            return CompletableFuture.completedFuture(ActionResult.success("ok"));
        }
    }

    private static final class TestConnection implements GatewayConnection {
        private final UserSession session;
        private final String remoteIp;
        private final List<GatewayPacket> packets = new ArrayList<>();
        private boolean open = true;

        private TestConnection(UserSession session, String remoteIp) {
            this.session = session;
            this.remoteIp = remoteIp;
        }

        @Override
        public UserSession userSession() {
            return session;
        }

        @Override
        public String protocolType() {
            return "tcp";
        }

        @Override
        public String remoteAddress() {
            return remoteIp + ":10000";
        }

        @Override
        public String remoteIp() {
            return remoteIp;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void send(GatewayPacket packet) {
            packets.add(packet);
        }

        @Override
        public void close() {
            open = false;
        }

        private GatewayPacket lastPacket() {
            return packets.getLast();
        }
    }
}
