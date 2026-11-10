package cn.itsub.game.rpc.demo;

import cn.itsub.game.rpc.demo.message.RpcDemoRequest;
import cn.itsub.game.rpc.demo.message.RpcDemoResponse;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class RpcActionCallerApp {
    private static final int CMD = 10;

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;

        ServiceNode client = new ServiceNode(host, port, "RpcActionCaller");
        client.start();
        try {
            callSuccess(client);
            callSuccess(client);
            callSuccess(client);
            callSuccess(client);
            callSuccess(client);
        } finally {
            client.shutdown();
        }
    }

    private static void callSuccess(ServiceNode client) {
        ActionResult result = client.invoke(CMD, 1, new RpcDemoRequest("hello"), InvokeOptions.builder()
                .sessionId(10_002L)
                .userId(10001L)
                .timeout(Duration.ofMillis(3000))
                .build());
        if (!result.isSuccess()) {
            log.error("[success-case] unexpected failure: code={}, message={}", result.getErrorCode(), result.getErrorMessage());
            return;
        }
        RpcDemoResponse response = result.getValue(RpcDemoResponse.class);
        log.info("[success-case] status={}, detail={}", response.getStatus(), response.getDetail());
    }
}
