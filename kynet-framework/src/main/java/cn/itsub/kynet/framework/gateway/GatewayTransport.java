package cn.itsub.kynet.framework.gateway;

public interface GatewayTransport extends AutoCloseable {
    GatewayEndpoint endpoint();

    void start();

    @Override
    void close();
}
