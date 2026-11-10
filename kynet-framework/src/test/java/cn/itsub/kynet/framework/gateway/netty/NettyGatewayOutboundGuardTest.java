package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import cn.itsub.kynet.framework.gateway.GatewayConnection;
import cn.itsub.kynet.framework.gateway.GatewayPacket;
import cn.itsub.kynet.framework.gateway.GatewayRuntime;
import cn.itsub.kynet.framework.gateway.GatewayServer;
import cn.itsub.kynet.framework.gateway.GatewaySlowClientPolicy;
import cn.itsub.kynet.framework.user.UserSession;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NettyGatewayOutboundGuardTest {

    @Test
    void pendingMessageLimitClosesSlowConnectionAndReleasesCapacity() {
        GatewayServer server = new GatewayServer(new StubServiceNode(), "tcp://127.0.0.1:9000");
        server.setMaxConnections(1);
        server.setMaxPendingWriteMessages(1);
        server.setMaxPendingWriteBytes(1024 * 1024);
        server.setMaxUnwritableDurationMillis(0);
        GatewayRuntime runtime = new GatewayRuntime(server);
        BlockingOutboundHandler blockingHandler = new BlockingOutboundHandler();
        EmbeddedChannel channel = new EmbeddedChannel(blockingHandler);
        TestConnection connection = new TestConnection(channel);

        assertTrue(runtime.tryRegisterConnection(connection));
        NettyGatewayOutboundGuard.initialize(channel, connection, runtime);

        GatewayPacket packet = GatewayPacket.push(1, 1, 1L, 1L, new byte[32]);
        assertTrue(NettyGatewayOutboundGuard.write(channel, packet, Integer.BYTES));
        assertEquals(1L, runtime.pendingOutboundMessages());

        assertFalse(NettyGatewayOutboundGuard.write(channel, packet, Integer.BYTES));
        assertFalse(connection.isOpen());
        assertEquals(1L, runtime.slowClientClosedCount());
        assertEquals(0, runtime.onlineConnectionCount());

        blockingHandler.failPendingWrites();
        channel.runPendingTasks();
        assertEquals(0L, runtime.pendingOutboundMessages());
        assertEquals(0L, runtime.pendingOutboundBytes());

        runtime.close();
        channel.finishAndReleaseAll();
    }

    @Test
    void sustainedUnwritableChannelClosesAfterConfiguredTimeout() throws InterruptedException {
        GatewayServer server = new GatewayServer(new StubServiceNode(), "tcp://127.0.0.1:9000");
        server.setMaxUnwritableDurationMillis(20);
        server.setSlowClientPolicy(GatewaySlowClientPolicy.BUFFER);
        GatewayRuntime runtime = new GatewayRuntime(server);
        EmbeddedChannel channel = new EmbeddedChannel();
        TestConnection connection = new TestConnection(channel);

        assertTrue(runtime.tryRegisterConnection(connection));
        NettyGatewayOutboundGuard.initialize(channel, connection, runtime);
        channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false);
        NettyGatewayOutboundGuard.channelWritabilityChanged(channel);

        Thread.sleep(40L);
        channel.runScheduledPendingTasks();

        assertFalse(connection.isOpen());
        assertEquals(1L, runtime.outboundBackpressureCount());
        assertEquals(1L, runtime.slowClientClosedCount());
        assertEquals(0, runtime.onlineConnectionCount());

        runtime.close();
        channel.finishAndReleaseAll();
    }

    private static final class BlockingOutboundHandler extends ChannelOutboundHandlerAdapter {
        private final List<ChannelPromise> pendingPromises = new ArrayList<>();

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            pendingPromises.add(promise);
        }

        private void failPendingWrites() {
            for (ChannelPromise promise : pendingPromises) {
                promise.tryFailure(new IllegalStateException("test release"));
            }
            pendingPromises.clear();
        }
    }

    private static final class TestConnection implements GatewayConnection {
        private final EmbeddedChannel channel;
        private final UserSession session = new UserSession(1L, "tcp", 1, "127.0.0.1:10000");

        private TestConnection(EmbeddedChannel channel) {
            this.channel = channel;
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
            return "127.0.0.1:10000";
        }

        @Override
        public String remoteIp() {
            return "127.0.0.1";
        }

        @Override
        public boolean isOpen() {
            return channel.isActive();
        }

        @Override
        public void send(GatewayPacket packet) {
            NettyGatewayOutboundGuard.write(channel, packet, Integer.BYTES);
        }

        @Override
        public void close() {
            channel.close();
        }
    }

    private static final class StubServiceNode extends ServiceNode {
        private StubServiceNode() {
            super("127.0.0.1", 1, "NettyGatewayOutboundGuardTest");
        }

        @Override
        public CompletableFuture<ActionResult> invokeAsync(int cmd, int act, Object payload, InvokeOptions options) {
            return CompletableFuture.completedFuture(ActionResult.success(null));
        }
    }
}
