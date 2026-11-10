package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.gateway.netty.NettyGatewayTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class GatewayLauncher {
    private static final Logger log = LoggerFactory.getLogger(GatewayLauncher.class);

    private GatewayLauncher() {
    }

    public static void run(ServiceNode serviceNode, GatewayServer gatewayServer) throws InterruptedException {
        GatewayRuntime runtime = new GatewayRuntime(gatewayServer);
        List<GatewayTransport> transports = new ArrayList<>();
        serviceNode.start();
        runtime.start();
        serviceNode.setGatewayPushHandler(pushMessage -> {
            runtime.handleBrokerPush(pushMessage);
            GatewayPushHandler gatewayPushHandler = gatewayServer.getGatewayPushHandler();
            if (gatewayPushHandler != null) {
                gatewayPushHandler.onPush(pushMessage);
            }
        });
        for (String endpointText : gatewayServer.getEndpoints()) {
            GatewayEndpoint endpoint = GatewayEndpoint.parse(endpointText);
            GatewayTransport transport = NettyGatewayTransportFactory.create(endpoint, runtime);
            transport.start();
            transports.add(transport);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(gatewayServer, runtime, transports), "gateway-shutdown"));
        log.info("Gateway started serviceId={} instanceId={} endpoints={}",
                serviceNode.getServiceId(), serviceNode.getInstanceId(), Arrays.toString(gatewayServer.getEndpoints()));
        new CountDownLatch(1).await();
    }

    private static void shutdown(GatewayServer gatewayServer, GatewayRuntime runtime, List<GatewayTransport> transports) {
        for (GatewayTransport transport : transports) {
            try {
                transport.close();
            } catch (Exception e) {
                log.warn("Close gateway transport failed endpoint={}", transport.endpoint().raw(), e);
            }
        }
        runtime.close();
        gatewayServer.getServiceNode().shutdown();
    }
}
