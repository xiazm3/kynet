package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.action.BaseAttachment;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import cn.itsub.kynet.framework.user.UserSession;
import cn.itsub.kynet.common.protocol.GatewayPushMessage;
import cn.itsub.kynet.common.protocol.PlayerBroadcastEnvelope;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayRuntimeSessionTest {

    @Test
    void gatewayTreatsPositiveLoginIdAsAuthenticated() {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        brokerClient.enqueue(ActionResult.success("login", attachmentWithUserId(1001L), true));
        brokerClient.enqueue(ActionResult.success("logout", attachmentWithUserId(0L), true));

        GatewayRuntime runtime = new GatewayRuntime(new GatewayServer(brokerClient, "tcp://127.0.0.1:9000"));
        TestGatewayConnection connection = new TestGatewayConnection(new UserSession(103L, "tcp"));

        runtime.handleIncoming(connection, GatewayPacket.request(4L, 1, 1, false, 0L,
                connection.userSession().getSessionId(), 0L, null, new byte[0]));

        assertTrue(connection.userSession().isAuthenticated());
        assertEquals(1001L, connection.userSession().getUserId());
        assertEquals(1001L, attachmentUserId(connection.userSession().getAttachmentData()));

        runtime.handleIncoming(connection, GatewayPacket.request(5L, 1, 1, false, 0L,
                connection.userSession().getSessionId(), 0L, null, new byte[0]));

        assertFalse(connection.userSession().isAuthenticated());
        assertEquals(0L, connection.userSession().getUserId());
        assertEquals(0L, attachmentUserId(connection.userSession().getAttachmentData()));
    }

    @Test
    void gatewayFiltersBroadcastEnvelopeByLocalPlayers() throws Exception {
        RecordingServiceNode brokerClient = new RecordingServiceNode();
        GatewayRuntime runtime = new GatewayRuntime(new GatewayServer(brokerClient, "tcp://127.0.0.1:9000"));

        TestGatewayConnection alice = new TestGatewayConnection(new UserSession(201L, "tcp", 11, "127.0.0.1"));
        alice.userSession().setUserId(10001L);
        TestGatewayConnection bob = new TestGatewayConnection(new UserSession(202L, "tcp", 11, "127.0.0.1"));
        bob.userSession().setUserId(10002L);
        TestGatewayConnection carol = new TestGatewayConnection(new UserSession(203L, "tcp", 11, "127.0.0.1"));
        carol.userSession().setUserId(10003L);

        runtime.registerConnection(alice);
        runtime.registerConnection(bob);
        runtime.registerConnection(carol);

        PlayerBroadcastEnvelope envelope = new PlayerBroadcastEnvelope();
        envelope.setMsgId(200001);
        envelope.setPlayerIds(List.of(10001L, 10003L));
        envelope.setPayload(cn.itsub.kynet.common.codec.BroadcastPayloadCodec.encode("hello"));

        GatewayPushMessage pushMessage = new GatewayPushMessage();
        pushMessage.setCmd(3);
        pushMessage.setAct(1);
        pushMessage.setPayload(ProtobufPayloadCodec.encode(envelope));

        runtime.handleBrokerPush(pushMessage);

        assertEquals(1, alice.packets.size());
        assertEquals(0, bob.packets.size());
        assertEquals(1, carol.packets.size());
        assertEquals("hello", com.google.protobuf.StringValue.parseFrom(alice.lastPacket().payload()).getValue());
    }

    private static byte[] attachmentWithUserId(long userId) {
        BaseAttachment attachment = new BaseAttachment();
        attachment.setUserId(userId);
        return ProtobufPayloadCodec.encode(attachment);
    }

    private static long attachmentUserId(byte[] attachmentData) {
        if (attachmentData == null || attachmentData.length == 0) {
            return 0L;
        }
        BaseAttachment attachment = ProtobufPayloadCodec.decode(attachmentData, BaseAttachment.class);
        return attachment != null ? attachment.getUserId() : 0L;
    }

    private static final class RecordingServiceNode extends ServiceNode {
        private final Deque<ActionResult> results = new ArrayDeque<>();

        private RecordingServiceNode() {
            super("127.0.0.1", 1, "GatewayRuntimeSessionTest");
        }

        private void enqueue(ActionResult result) {
            results.addLast(result);
        }

        @Override
        public CompletableFuture<ActionResult> invokeAsync(int cmd, int act, Object payload, InvokeOptions options) {
            ActionResult result = results.isEmpty() ? ActionResult.success("default") : results.removeFirst();
            return CompletableFuture.completedFuture(result);
        }
    }

    private static final class TestGatewayConnection implements GatewayConnection {
        private final UserSession session;
        private final List<GatewayPacket> packets = new ArrayList<>();
        private boolean open = true;

        private TestGatewayConnection(UserSession session) {
            this.session = session;
        }

        @Override
        public UserSession userSession() {
            return session;
        }

        @Override
        public String protocolType() {
            return session.getProtocolType();
        }

        @Override
        public String remoteAddress() {
            return "127.0.0.1";
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
            return packets.isEmpty() ? null : packets.getLast();
        }
    }
}
