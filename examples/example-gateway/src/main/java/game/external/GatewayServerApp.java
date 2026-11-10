package game.external;

import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.gateway.GatewayLauncher;
import cn.itsub.kynet.framework.gateway.GatewayServer;
import cn.itsub.kynet.framework.gateway.GatewayTlsConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GatewayServerApp {
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;

        ServiceNode client = new ServiceNode(host, port, "GatewayService");

        GatewayServer server = new GatewayServer(
                client,
                System.getProperty("kynet.gateway.tcpEndpoint", "tcp://0.0.0.0:30001"),
                System.getProperty("kynet.gateway.udpEndpoint", "udp://0.0.0.0:30002"),
                System.getProperty("kynet.gateway.wsEndpoint", "ws://0.0.0.0:30003/ws")
        );
        configureTlsIfPresent(server);
        server.setUserHook(new ExternalUserHook());
        server.setGatewayPushHandler(push -> {
            log.info(
                    "gateway push cmd={} act={} sessionId={} userId={} payload={}",
                    push.getCmd(), push.getAct(), push.getSessionId(), push.getUserId(), push.getPayload()
            );
        });
        server.getAccessControl().auth(1, 2);
        server.getAccessControl().auth(1, 4);
        server.getAccessControl().auth(2, 3);
        server.getAccessControl().deny(4);
        server.getAccessControl().allow(6, 1);

        GatewayLauncher.run(client, server);
    }

    private static void configureTlsIfPresent(GatewayServer server) {
        String certificate = System.getProperty("kynet.gateway.tls.certificate");
        String privateKey = System.getProperty("kynet.gateway.tls.privateKey");
        if (certificate == null || certificate.isBlank() || privateKey == null || privateKey.isBlank()) {
            return;
        }
        server.setTlsConfig(GatewayTlsConfig.of(
                certificate,
                privateKey,
                System.getProperty("kynet.gateway.tls.privateKeyPassword")));
    }
}
