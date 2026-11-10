package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.gateway.GatewayConnection;
import cn.itsub.kynet.framework.gateway.GatewayEndpoint;
import cn.itsub.kynet.framework.gateway.GatewayPacket;
import cn.itsub.kynet.framework.gateway.GatewayPacketCodec;
import cn.itsub.kynet.framework.gateway.GatewayRuntime;
import cn.itsub.kynet.framework.gateway.GatewayTransport;
import cn.itsub.kynet.framework.user.UserSession;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.net.InetSocketAddress;

public class TcpGatewayTransport implements GatewayTransport {
    private static final Logger log = LoggerFactory.getLogger(TcpGatewayTransport.class);
    private static final AttributeKey<GatewayConnection> CONNECTION_KEY = AttributeKey.valueOf("gatewayConnection");

    private final GatewayEndpoint endpoint;
    private final GatewayRuntime runtime;
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup();
    private Channel serverChannel;

    public TcpGatewayTransport(GatewayEndpoint endpoint, GatewayRuntime runtime) {
        this.endpoint = endpoint;
        this.runtime = runtime;
    }

    @Override
    public GatewayEndpoint endpoint() {
        return endpoint;
    }

    @Override
    public void start() {
        SslContext sslContext = NettyGatewaySslSupport.buildServerContext(endpoint, runtime.gatewayServer());
        int maxFrameLength = runtime.gatewayServer().getMaxFrameLength();
        int maxPayloadLength = runtime.gatewayServer().getMaxPayloadLength();
        int maxStringLength = runtime.gatewayServer().getMaxStringLength();
        WriteBufferWaterMark writeBufferWaterMark = new WriteBufferWaterMark(
                runtime.gatewayServer().getWriteBufferLowWaterMark(),
                runtime.gatewayServer().getWriteBufferHighWaterMark());
        try {
            ChannelFuture future = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufferWaterMark)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            ChannelPipeline pipeline = channel.pipeline();
                            if (sslContext != null) {
                                pipeline.addLast(sslContext.newHandler(channel.alloc()));
                            }
                            pipeline
                                    .addLast(new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new MessageToMessageDecoder<ByteBuf>() {
                                        @Override
                                        protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) {
                                            out.add(GatewayPacketCodec.decode(msg, maxPayloadLength, maxStringLength));
                                        }
                                    })
                                    .addLast(new MessageToByteEncoder<GatewayPacket>() {
                                        @Override
                                        protected void encode(ChannelHandlerContext ctx, GatewayPacket msg, ByteBuf out) {
                                            GatewayPacketCodec.encode(out, msg);
                                        }
                                    })
                                    .addLast(new ChannelInboundHandlerAdapter() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                            NettyChannelConnection connection = new NettyChannelConnection(
                                                    "tcp",
                                                    ctx.channel(),
                                                    runtime.newSession("tcp", String.valueOf(ctx.channel().remoteAddress())),
                                                    runtime);
                                            if (runtime.tryRegisterConnection(connection)) {
                                                ctx.channel().attr(CONNECTION_KEY).set(connection);
                                                NettyGatewayOutboundGuard.initialize(ctx.channel(), connection, runtime);
                                            } else {
                                                ctx.close();
                                                return;
                                            }
                                            super.channelActive(ctx);
                                        }

                                        @Override
                                        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                            NettyGatewayOutboundGuard.channelInactive(ctx.channel());
                                            GatewayConnection connection = ctx.channel().attr(CONNECTION_KEY).get();
                                            if (connection != null) {
                                                runtime.unregisterConnection(connection);
                                            }
                                            super.channelInactive(ctx);
                                        }

                                        @Override
                                        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
                                            NettyGatewayOutboundGuard.channelWritabilityChanged(ctx.channel());
                                            super.channelWritabilityChanged(ctx);
                                        }

                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                            log.warn("TCP gateway channel error remote={}", ctx.channel().remoteAddress(), cause);
                                            ctx.close();
                                        }
                                    })
                                    .addLast(new SimpleChannelInboundHandler<GatewayPacket>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, GatewayPacket msg) {
                                            GatewayConnection connection = ctx.channel().attr(CONNECTION_KEY).get();
                                            if (connection != null) {
                                                runtime.handleIncoming(connection, msg);
                                            }
                                        }
                                    });
                        }
                    })
                    .bind(endpoint.host(), endpoint.port())
                    .sync();
            serverChannel = future.channel();
            log.info("Netty TCP gateway listening on {}", endpoint.raw());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting TCP gateway transport " + endpoint.raw(), e);
        }
    }

    @Override
    public void close() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private record NettyChannelConnection(String protocolType, Channel channel, UserSession userSession,
                                          GatewayRuntime runtime)
            implements GatewayConnection {

        @Override
        public String remoteAddress() {
            return String.valueOf(channel.remoteAddress());
        }

        @Override
        public String remoteIp() {
            return channel.remoteAddress() instanceof InetSocketAddress address
                    ? address.getAddress().getHostAddress()
                    : remoteAddress();
        }

        @Override
        public boolean isOpen() {
            return channel.isActive();
        }

        @Override
        public void send(GatewayPacket packet) {
            if (isOpen()) {
                NettyGatewayOutboundGuard.write(channel, packet, Integer.BYTES);
            }
        }

        @Override
        public void close() {
            channel.close();
        }
    }
}
