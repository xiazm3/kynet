package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.gateway.GatewayEndpoint;
import cn.itsub.kynet.framework.gateway.GatewayServer;
import cn.itsub.kynet.framework.gateway.GatewayTlsConfig;
import io.netty.handler.ssl.SslContext;

final class NettyGatewaySslSupport {
    private NettyGatewaySslSupport() {
    }

    static SslContext buildServerContext(GatewayEndpoint endpoint, GatewayServer server) {
        if (!endpoint.secure()) {
            return null;
        }
        GatewayTlsConfig tlsConfig = server.getTlsConfig();
        if (tlsConfig == null) {
            throw new IllegalStateException("Secure Gateway endpoint requires GatewayTlsConfig: " + endpoint.raw());
        }
        return tlsConfig.buildServerContext();
    }
}
