package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.gateway.GatewayEndpoint;
import cn.itsub.kynet.framework.gateway.GatewayProtocol;
import cn.itsub.kynet.framework.gateway.GatewayRuntime;
import cn.itsub.kynet.framework.gateway.GatewayTransport;

public final class NettyGatewayTransportFactory {
    private NettyGatewayTransportFactory() {
    }

    public static GatewayTransport create(GatewayEndpoint endpoint, GatewayRuntime runtime) {
        return switch (endpoint.protocol()) {
            case TCP -> new TcpGatewayTransport(endpoint, runtime);
            case WS -> new WebSocketGatewayTransport(endpoint, runtime);
            case UDP -> new UdpGatewayTransport(endpoint, runtime);
            case KCP -> throw new IllegalArgumentException("KCP transport is not implemented yet: " + endpoint.raw());
        };
    }
}
