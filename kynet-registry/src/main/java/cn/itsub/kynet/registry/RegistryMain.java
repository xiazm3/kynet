package cn.itsub.kynet.registry;

import java.util.concurrent.CountDownLatch;

public final class RegistryMain {

    private RegistryMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8888;
        RegistryServer server = new RegistryServer(port);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        try {
            new CountDownLatch(1).await();
        } finally {
            server.stop();
        }
    }
}
