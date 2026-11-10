package cn.itsub.game.rpc.demo;

import cn.itsub.game.rpc.demo.action.RpcDemoAction;
import cn.itsub.game.rpc.demo.action.RpcThroughputAction;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.client.ServiceNode;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class RpcActionProviderApp {

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;

        ServiceNode client = new ServiceNode(host, port, "RpcActionProvider");
        client.start();

        ActionBootstrap bootstrap = new ActionBootstrap(client);
        bootstrap.registerAction(new RpcDemoAction());
        bootstrap.registerAction(new RpcThroughputAction());

        log.info("RPC action provider started, serviceId={}, instanceId={}, actions=[10:1,99:1]",
                client.getServiceId(), client.getInstanceId());
        new CountDownLatch(1).await();
    }
}
