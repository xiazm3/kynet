import cn.itsub.game.login.action.LoginAction;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.gateway.*;
import cn.itsub.kynet.framework.gateway.netty.NettyGatewayTransportFactory;
import cn.itsub.kynet.registry.RegistryServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

// Runs the repository's real registry, LoginAction and Netty transports in one JVM.
public class GatewayInteropServer {
    public static void main(String[] args) throws Exception {
        int registryPort = Integer.parseInt(args[0]);
        RegistryServer registry = new RegistryServer(registryPort);
        ServiceNode login = new ServiceNode("127.0.0.1", registryPort, "LoginService");
        ServiceNode gateway = new ServiceNode("127.0.0.1", registryPort, "GatewayService");
        List<GatewayTransport> transports = new ArrayList<>();
        GatewayRuntime runtime = null;
        try {
            registry.start();
            login.start();
            new ActionBootstrap(login).registerAction(new LoginAction());
            gateway.start();
            GatewayServer server = new GatewayServer(gateway,
                "tcp://127.0.0.1:" + args[1], "udp://127.0.0.1:" + args[2],
                "ws://127.0.0.1:" + args[3] + "/ws");
            server.getAccessControl().auth(1, 2);
            server.getAccessControl().auth(1, 4);
            runtime = new GatewayRuntime(server);
            runtime.start();
            gateway.setGatewayPushHandler(runtime::handleBrokerPush);
            for (String endpoint : server.getEndpoints()) {
                GatewayTransport transport = NettyGatewayTransportFactory.create(GatewayEndpoint.parse(endpoint), runtime);
                transports.add(transport);
                transport.start();
            }
            Files.writeString(Path.of(args[4]), "ready");
            while (!Files.exists(Path.of(args[5]))) Thread.sleep(100);
        } finally {
            for (GatewayTransport transport : transports) transport.close();
            if (runtime != null) runtime.close();
            gateway.shutdown();
            login.shutdown();
            registry.stop();
        }
    }
}
