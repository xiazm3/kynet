package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.gateway.GatewayConnection;
import cn.itsub.kynet.framework.gateway.GatewayEndpoint;
import cn.itsub.kynet.framework.gateway.GatewayPacket;
import cn.itsub.kynet.framework.gateway.GatewayPacketCodec;
import cn.itsub.kynet.framework.gateway.GatewayRuntime;
import cn.itsub.kynet.framework.gateway.GatewayTransport;
import cn.itsub.kynet.framework.user.UserSession;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UdpGatewayTransport implements GatewayTransport {
    private static final Logger log = LoggerFactory.getLogger(UdpGatewayTransport.class);

    private final GatewayEndpoint endpoint;
    private final GatewayRuntime runtime;
    private final NioEventLoopGroup group = new NioEventLoopGroup(1);
    private final Map<InetSocketAddress, UdpPeerConnection> peers = new ConcurrentHashMap<>();
    private Channel channel;

    public UdpGatewayTransport(GatewayEndpoint endpoint, GatewayRuntime runtime) {
        this.endpoint = endpoint;
        this.runtime = runtime;
    }

    @Override
    public GatewayEndpoint endpoint() {
        return endpoint;
    }

    @Override
    public void start() {
        int maxPayloadLength = runtime.gatewayServer().getMaxPayloadLength();
        int maxStringLength = runtime.gatewayServer().getMaxStringLength();
        WriteBufferWaterMark writeBufferWaterMark = new WriteBufferWaterMark(
                runtime.gatewayServer().getWriteBufferLowWaterMark(),
                runtime.gatewayServer().getWriteBufferHighWaterMark());
        try {
            channel = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufferWaterMark)
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel ch) {
                            ch.pipeline().addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) {
                                    UdpPeerConnection connection = peers.computeIfAbsent(msg.sender(), address -> {
                                        UdpPeerConnection newConnection = new UdpPeerConnection(
                                                "udp",
                                                ctx.channel(),
                                                address,
                                                runtime.newSession("udp", String.valueOf(address)));
                                        return runtime.tryRegisterConnection(newConnection) ? newConnection : null;
                                    });
                                    if (connection == null) {
                                        return;
                                    }
                                    try {
                                        runtime.handleIncoming(connection, GatewayPacketCodec.decode(
                                                msg.content(), maxPayloadLength, maxStringLength));
                                    } catch (RuntimeException e) {
                                        peers.remove(msg.sender(), connection);
                                        runtime.unregisterConnection(connection);
                                        log.warn("Drop invalid UDP gateway packet remote={} reason={}",
                                                msg.sender(), e.getMessage());
                                    }
                                }

                                @Override
                                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                    log.warn("UDP gateway channel error local={}", endpoint.raw(), cause);
                                }
                            });
                        }
                    })
                    .bind(endpoint.host(), endpoint.port())
                    .sync()
                    .channel();
            log.info("Netty UDP gateway listening on {}", endpoint.raw());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting UDP gateway transport " + endpoint.raw(), e);
        }
    }

    @Override
    public void close() {
        for (UdpPeerConnection connection : peers.values()) {
            runtime.unregisterConnection(connection);
        }
        peers.clear();
        if (channel != null) {
            channel.close();
        }
        group.shutdownGracefully();
    }

    private final class UdpPeerConnection implements GatewayConnection {
        private final String protocolType;
        private final Channel channel;
        private final InetSocketAddress remoteAddress;
        private final UserSession userSession;

        private UdpPeerConnection(String protocolType, Channel channel, InetSocketAddress remoteAddress, UserSession userSession) {
            this.protocolType = protocolType;
            this.channel = channel;
            this.remoteAddress = remoteAddress;
            this.userSession = userSession;
        }

        @Override
        public UserSession userSession() {
            return userSession;
        }

        @Override
        public String protocolType() {
            return protocolType;
        }

        @Override
        public String remoteAddress() {
            return String.valueOf(remoteAddress);
        }

        @Override
        public String remoteIp() {
            return remoteAddress.getAddress().getHostAddress();
        }

        @Override
        public boolean isOpen() {
            return channel.isActive() && peers.containsKey(remoteAddress);
        }

        @Override
        public void send(GatewayPacket packet) {
            if (!isOpen()) {
                return;
            }
            if (!channel.isWritable()) {
                runtime.recordOutboundBackpressure();
                runtime.recordOutboundDrop();
                return;
            }
            ByteBuf buffer = channel.alloc().buffer();
            GatewayPacketCodec.encode(buffer, packet);
            channel.writeAndFlush(new DatagramPacket(buffer, remoteAddress));
        }

        @Override
        public void close() {
            peers.remove(remoteAddress, this);
        }
    }
}
