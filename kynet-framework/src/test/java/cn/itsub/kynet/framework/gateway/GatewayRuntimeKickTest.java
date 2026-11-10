package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import cn.itsub.kynet.framework.user.UserHook;
import cn.itsub.kynet.framework.user.UserSession;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayRuntimeKickTest {

    @Test
    void kickBySessionIdClosesOnlyTargetAndFiresQuitHook() {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        GatewayServer server = new GatewayServer(brokerClient, "tcp://127.0.0.1:9000");
        List<Long> quitSessions = new ArrayList<>();
        server.setUserHook(new UserHook() {
            @Override public void into(UserSession session) { }
            @Override public void quit(UserSession session) { quitSessions.add(session.getSessionId()); }
        });
        GatewayRuntime runtime = new GatewayRuntime(server);

        TestGatewayConnection alice = connection(runtime, 201L, 10001L);
        TestGatewayConnection bob = connection(runtime, 202L, 10002L);

        runtime.handleGatewayKick(201L, 0L, 0L, "测试踢人");

        assertFalse(alice.open);
        assertTrue(bob.open);
        assertEquals(List.of(201L), quitSessions);
        assertEquals(1, runtime.kickedConnectionCount());
    }

    @Test
    void kickByUserIdClosesAllConnectionsOfUser() {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        GatewayServer server = new GatewayServer(brokerClient, "tcp://127.0.0.1:9000");
        List<Long> quitSessions = new ArrayList<>();
        server.setUserHook(new UserHook() {
            @Override public void into(UserSession session) { }
            @Override public void quit(UserSession session) { quitSessions.add(session.getSessionId()); }
        });
        GatewayRuntime runtime = new GatewayRuntime(server);

        TestGatewayConnection first = connection(runtime, 301L, 10001L);
        TestGatewayConnection second = connection(runtime, 302L, 10001L);
        TestGatewayConnection other = connection(runtime, 303L, 10002L);

        runtime.handleGatewayKick(0L, 10001L, 0L, "封号");

        assertFalse(first.open);
        assertFalse(second.open);
        assertTrue(other.open);
        assertEquals(2, runtime.kickedConnectionCount());
        assertEquals(2, quitSessions.size());
    }

    @Test
    void kickUserExceptKeepsSpecifiedSession() {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        GatewayServer server = new GatewayServer(brokerClient, "tcp://127.0.0.1:9000");
        GatewayRuntime runtime = new GatewayRuntime(server);

        TestGatewayConnection current = connection(runtime, 401L, 10001L);
        TestGatewayConnection stale = connection(runtime, 402L, 10001L);

        runtime.handleGatewayKick(0L, 10001L, 401L, "顶号");

        assertTrue(current.open);
        assertFalse(stale.open);
        assertEquals(1, runtime.kickedConnectionCount());
    }

    @Test
    void kickWithoutMatchingTargetIsNoop() {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        GatewayRuntime runtime = new GatewayRuntime(new GatewayServer(brokerClient, "tcp://127.0.0.1:9000"));

        runtime.handleGatewayKick(999L, 0L, 0L, null);
        runtime.handleGatewayKick(0L, 999L, 0L, null);
        runtime.handleGatewayKick(0L, 0L, 0L, null);

        assertEquals(0, runtime.kickedConnectionCount());
    }

    private static TestGatewayConnection connection(GatewayRuntime runtime, long sessionId, long userId) {
        UserSession session = new UserSession(sessionId, "tcp", 11, "127.0.0.1");
        session.setUserId(userId);
        TestGatewayConnection connection = new TestGatewayConnection(session);
        runtime.registerConnection(connection);
        return connection;
    }

    private static final class RecordingServiceNode extends ServiceNode {
        private final Deque<ActionResult> results = new ArrayDeque<>();

        private RecordingServiceNode() {
            super("127.0.0.1", 1, "GatewayRuntimeKickTest");
        }

        @Override
        public CompletableFuture<ActionResult> invokeAsync(int cmd, int act, Object payload, InvokeOptions options) {
            ActionResult result = results.isEmpty() ? ActionResult.success("default") : results.removeFirst();
            return CompletableFuture.completedFuture(result);
        }
    }

    private static final class TestGatewayConnection implements GatewayConnection {
        private final UserSession session;
        private boolean open = true;

        private TestGatewayConnection(UserSession session) {
            this.session = session;
        }

        @Override public UserSession userSession() { return session; }
        @Override public String protocolType() { return session.getProtocolType(); }
        @Override public String remoteAddress() { return "127.0.0.1"; }
        @Override public boolean isOpen() { return open; }
        @Override public void send(GatewayPacket packet) { }
        @Override public void close() { open = false; }
    }
}
