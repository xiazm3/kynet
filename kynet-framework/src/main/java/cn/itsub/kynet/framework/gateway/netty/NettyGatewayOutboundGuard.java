package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.gateway.GatewayConnection;
import cn.itsub.kynet.framework.gateway.GatewayPacket;
import cn.itsub.kynet.framework.gateway.GatewayPacketCodec;
import cn.itsub.kynet.framework.gateway.GatewayRuntime;
import cn.itsub.kynet.framework.gateway.GatewayServer;
import cn.itsub.kynet.framework.gateway.GatewaySlowClientPolicy;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class NettyGatewayOutboundGuard {
    private static final AttributeKey<ConnectionState> STATE_KEY =
            AttributeKey.valueOf("gatewayOutboundGuard");

    private NettyGatewayOutboundGuard() {
    }

    static void initialize(Channel channel, GatewayConnection connection, GatewayRuntime runtime) {
        ConnectionState state = new ConnectionState(channel, connection, runtime);
        ConnectionState existing = channel.attr(STATE_KEY).setIfAbsent(state);
        if (existing != null) {
            throw new IllegalStateException("Gateway outbound guard is already initialized");
        }
        state.onWritabilityChanged();
    }

    static boolean write(Channel channel, GatewayPacket packet, int framingOverheadBytes) {
        ConnectionState state = channel.attr(STATE_KEY).get();
        return state != null && state.write(packet, framingOverheadBytes);
    }

    static void channelWritabilityChanged(Channel channel) {
        ConnectionState state = channel.attr(STATE_KEY).get();
        if (state != null) {
            state.onWritabilityChanged();
        }
    }

    static void channelInactive(Channel channel) {
        ConnectionState state = channel.attr(STATE_KEY).get();
        if (state != null) {
            state.deactivate();
        }
    }

    private static final class ConnectionState {
        private final Channel channel;
        private final GatewayConnection connection;
        private final GatewayRuntime runtime;
        private final GatewayServer gatewayServer;
        private final GatewayOutboundState outboundState = new GatewayOutboundState();
        private final AtomicBoolean active = new AtomicBoolean(true);

        private ConnectionState(Channel channel, GatewayConnection connection, GatewayRuntime runtime) {
            this.channel = channel;
            this.connection = connection;
            this.runtime = runtime;
            this.gatewayServer = runtime.gatewayServer();
        }

        private boolean write(GatewayPacket packet, int framingOverheadBytes) {
            if (!active.get() || !connection.isOpen()) {
                return false;
            }
            if (!channel.isWritable()) {
                onWritabilityChanged();
                if (!active.get()) {
                    return false;
                }
                GatewaySlowClientPolicy policy = gatewayServer.getSlowClientPolicy();
                if (policy == GatewaySlowClientPolicy.DROP) {
                    runtime.recordOutboundDrop();
                    return false;
                }
                if (policy == GatewaySlowClientPolicy.CLOSE) {
                    return false;
                }
            }

            int encodedBytes = Math.addExact(GatewayPacketCodec.encodedLength(packet), framingOverheadBytes);
            if (!outboundState.tryAcquire(
                    encodedBytes,
                    gatewayServer.getMaxPendingWriteBytes(),
                    gatewayServer.getMaxPendingWriteMessages())) {
                closeSlowConnection("pending write limit exceeded");
                return false;
            }
            runtime.outboundWriteQueued(encodedBytes);
            try {
                ChannelFuture future = channel.writeAndFlush(packet);
                future.addListener(ignored -> release(encodedBytes));
                return true;
            } catch (RuntimeException | Error e) {
                release(encodedBytes);
                throw e;
            }
        }

        private void release(int encodedBytes) {
            outboundState.release(encodedBytes);
            runtime.outboundWriteCompleted(encodedBytes);
        }

        private void onWritabilityChanged() {
            if (!active.get()) {
                return;
            }
            if (channel.isWritable()) {
                outboundState.markWritable();
                return;
            }
            long nowNanos = System.nanoTime();
            if (outboundState.markUnwritable(nowNanos)) {
                runtime.recordOutboundBackpressure();
                scheduleUnwritableTimeout(nowNanos);
            }
            if (gatewayServer.getSlowClientPolicy() == GatewaySlowClientPolicy.CLOSE) {
                closeSlowConnection("channel became unwritable");
            }
        }

        private void scheduleUnwritableTimeout(long unwritableSinceNanos) {
            long timeoutMillis = gatewayServer.getMaxUnwritableDurationMillis();
            if (timeoutMillis <= 0L) {
                return;
            }
            channel.eventLoop().schedule(
                    () -> checkUnwritableTimeout(unwritableSinceNanos),
                    timeoutMillis,
                    TimeUnit.MILLISECONDS);
        }

        private void checkUnwritableTimeout(long expectedUnwritableSinceNanos) {
            if (!active.get() || !connection.isOpen()) {
                return;
            }
            if (channel.isWritable()) {
                outboundState.markWritable();
                return;
            }
            if (outboundState.unwritableSinceNanos() != expectedUnwritableSinceNanos) {
                return;
            }
            long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(
                    gatewayServer.getMaxUnwritableDurationMillis());
            long nowNanos = System.nanoTime();
            if (outboundState.isUnwritableTimedOut(nowNanos, timeoutNanos)) {
                closeSlowConnection("channel remained unwritable beyond timeout");
                return;
            }
            if (timeoutNanos > 0L) {
                long remainingNanos = Math.max(1L, timeoutNanos - (nowNanos - expectedUnwritableSinceNanos));
                channel.eventLoop().schedule(
                        () -> checkUnwritableTimeout(expectedUnwritableSinceNanos),
                        remainingNanos,
                        TimeUnit.NANOSECONDS);
            }
        }

        private void closeSlowConnection(String reason) {
            if (!active.compareAndSet(true, false)) {
                return;
            }
            runtime.closeSlowConnection(
                    connection,
                    reason,
                    outboundState.pendingBytes(),
                    outboundState.pendingMessages());
        }

        private void deactivate() {
            active.set(false);
            outboundState.markWritable();
        }
    }
}
