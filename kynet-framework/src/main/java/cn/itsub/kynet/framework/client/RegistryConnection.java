package cn.itsub.kynet.framework.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** One bounded TCP control connection; business traffic stays in PeerTransport. */
final class RegistryConnection implements AutoCloseable {
    private static final int MAX_FRAME_BYTES = Math.max(64 * 1024,
            Integer.getInteger("kynet.registry.maxFrameBytes", 1024 * 1024));
    private static final long MAX_PENDING_BYTES = Math.max(MAX_FRAME_BYTES,
            Long.getLong("kynet.registry.clientQueueBytes", 1024L * 1024));

    private final FrameHandler frames;
    private final DisconnectHandler disconnects;
    private final AtomicLong pendingBytes = new AtomicLong();
    private NioEventLoopGroup group;
    private volatile Channel channel;
    private volatile long generation;
    private boolean closed;

    RegistryConnection(FrameHandler frames, DisconnectHandler disconnects) {
        this.frames = frames;
        this.disconnects = disconnects;
    }

    synchronized void connect(String host, int port, long connectionGeneration) {
        if (closed) throw new IllegalStateException("Registry connection is closed");
        if (group == null) group = new NioEventLoopGroup(1);
        int timeoutMs = Math.max(100, Integer.getInteger("kynet.registry.connectTimeoutMs", 1_000));
        Channel connected = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutMs)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(64 * 1024, 256 * 1024))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel socket) {
                        socket.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_BYTES, 0, 4, 0, 4))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                    @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
                                        byte[] bytes = new byte[frame.readableBytes()];
                                        frame.readBytes(bytes);
                                        frames.onFrame(bytes, connectionGeneration);
                                    }
                                    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                        disconnects.onDisconnect(connectionGeneration, "Registry TCP connection closed");
                                        super.channelInactive(ctx);
                                    }
                                    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        disconnects.onDisconnect(connectionGeneration,
                                                "Registry TCP connection failed: " + cause);
                                        ctx.close();
                                    }
                                });
                    }
                })
                .connect(host, port).syncUninterruptibly().channel();
        channel = connected;
        generation = connectionGeneration;
    }

    void send(DirectBuffer buffer, int offset, int length) {
        Channel current = channel;
        if (current == null || !current.isActive()) throw new SendException("Registry TCP connection is unavailable", true);
        if (length <= 0 || length > MAX_FRAME_BYTES - Integer.BYTES) {
            throw new IllegalArgumentException("Registry frame exceeds maximum size: " + length);
        }
        long pending;
        do {
            pending = pendingBytes.get();
            if (pending + length > MAX_PENDING_BYTES) throw new SendException("Registry TCP send queue is full", false);
        } while (!pendingBytes.compareAndSet(pending, pending + length));

        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        try {
            long connectionGeneration = generation;
            current.writeAndFlush(Unpooled.wrappedBuffer(bytes)).addListener(future -> {
                pendingBytes.addAndGet(-length);
                if (!future.isSuccess()) {
                    disconnects.onDisconnect(connectionGeneration, "Registry TCP write failed: " + future.cause());
                    current.close();
                }
            });
        } catch (RuntimeException e) {
            pendingBytes.addAndGet(-length);
            throw new SendException("Registry TCP write failed: " + e, true);
        }
    }

    boolean isConnected() {
        Channel current = channel;
        return current != null && current.isActive();
    }

    synchronized void disconnect() {
        Channel current = channel;
        channel = null;
        if (current != null) current.close();
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        disconnect();
        if (group != null) group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    @FunctionalInterface interface FrameHandler { void onFrame(byte[] frame, long generation); }
    @FunctionalInterface interface DisconnectHandler { void onDisconnect(long generation, String reason); }

    static final class SendException extends IllegalStateException {
        private final boolean fatal;
        SendException(String message, boolean fatal) { super(message); this.fatal = fatal; }
        boolean fatal() { return fatal; }
    }
}
