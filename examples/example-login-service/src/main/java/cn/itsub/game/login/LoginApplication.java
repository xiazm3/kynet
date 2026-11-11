package cn.itsub.game.login;

import cn.itsub.game.login.action.LoginAction;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.client.ServiceNode;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class LoginApplication {
    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;

        ServiceNode client = new ServiceNode(host, port, "LoginService");
        client.start();

        ActionBootstrap bootstrap = new ActionBootstrap(client);
        bootstrap.registerAction(new LoginAction());
        bootstrap.registerAction(new cn.itsub.game.login.action.SignAction());

        log.info("login service started, serviceId={}, instanceId={}", client.getServiceId(), client.getInstanceId());
        new CountDownLatch(1).await();
    }
}
