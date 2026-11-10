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
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.net.InetSocketAddress;

public class WebSocketGatewayTransport implements GatewayTransport {
    private static final Logger log = LoggerFactory.getLogger(WebSocketGatewayTransport.class);
    private static final AttributeKey<GatewayConnection> CONNECTION_KEY = AttributeKey.valueOf("gatewayWsConnection");

    private final GatewayEndpoint endpoint;
    private final GatewayRuntime runtime;
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup();
    private Channel serverChannel;

    public WebSocketGatewayTransport(GatewayEndpoint endpoint, GatewayRuntime runtime) {
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
                                    .addLast(new HttpServerCodec())
                                    .addLast(new HttpObjectAggregator(65_536))
                                    .addLast(new WebSocketOriginHandler(runtime.gatewayServer()))
                                    .addLast(new WebSocketServerProtocolHandler(endpoint.path(), null, true, maxFrameLength))
                                    .addLast(new MessageToMessageDecoder<BinaryWebSocketFrame>() {
                                        @Override
                                        protected void decode(ChannelHandlerContext ctx, BinaryWebSocketFrame msg, List<Object> out) {
                                            out.add(GatewayPacketCodec.decode(
                                                    msg.content(), maxPayloadLength, maxStringLength));
                                        }
                                    })
                                    .addLast(new MessageToMessageEncoder<GatewayPacket>() {
                                        @Override
                                        protected void encode(ChannelHandlerContext ctx, GatewayPacket msg, List<Object> out) {
                                            ByteBuf buffer = ctx.alloc().buffer();
                                            GatewayPacketCodec.encode(buffer, msg);
                                            out.add(new BinaryWebSocketFrame(buffer));
                                        }
                                    })
                                    .addLast(new ChannelInboundHandlerAdapter() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                            NettyWsConnection connection = new NettyWsConnection(
                                                    "ws",
                                                    ctx.channel(),
                                                    runtime.newSession("ws", String.valueOf(ctx.channel().remoteAddress())),
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
                                        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                            super.userEventTriggered(ctx, evt);
                                        }

                                        @Override
                                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                            log.warn("WS gateway channel error remote={}", ctx.channel().remoteAddress(), cause);
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
            log.info("Netty WebSocket gateway listening on {}", endpoint.raw());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting WebSocket gateway transport " + endpoint.raw(), e);
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

    private record NettyWsConnection(String protocolType, Channel channel, UserSession userSession,
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
                NettyGatewayOutboundGuard.write(channel, packet, 10);
            }
        }

        @Override
        public void close() {
            channel.close();
        }
    }
}
