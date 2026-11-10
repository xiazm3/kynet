package cn.itsub.game.login;

import cn.itsub.game.login.action.LoginAttachment;
import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import game.message.LoginMessage;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

/**
 * 验证 attachment 传播链：A 调用 B，B 设置 attachment 并调用 C，
 * C 修改 attachment，最终 A 验证 B 和 C 的结果都带回。
 */
@Slf4j
public class LoginActionCallerApp {
    private static final int CMD = 1;
    private static final int ACT_LOGIN = 1;

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;
        long sessionId = args.length > 2 ? Long.parseLong(args[2]) : 1001L;
        long userId = args.length > 3 ? Long.parseLong(args[3]) : 0L;

        ServiceNode client = new ServiceNode(host, port, "LoginActionCaller");
        client.start();
        try {
            LoginActionCallerApp app = new LoginActionCallerApp();
            app.testAttachmentChain(client, sessionId, userId);
        } finally {
            client.shutdown();
        }
    }

    /**
     * A 调用 login (B)，B 会设置 attachment 并内部调用 hello3 (C)，
     * C 修改 attachment。最后 A 验证 attachment 中同时包含 B 和 C 的修改。
     */
    private void testAttachmentChain(ServiceNode client, long sessionId, long userId) {
        LoginMessage request = new LoginMessage("player1", "123456");
        ActionResult result = client.invoke(CMD, ACT_LOGIN, request, InvokeOptions.builder()
                .sessionId(sessionId)
                .userId(userId)
                .timeout(Duration.ofMillis(3000))
                .build());

        if (!result.isSuccess()) {
            log.error("[A] login failed: code={}, message={}", result.getErrorCode(), result.getErrorMessage());
            return;
        }

        LoginMessage response = result.getValue(LoginMessage.class);
        log.info("[A] login success: username={}, token={}", response.getUsername(), response.getToken());

        // 验证 attachment 是否带回
        if (!result.isAttachmentUpdated()) {
            log.warn("[A] attachment NOT updated — 链路未带回 attachment");
            return;
        }

        byte[] attachmentBytes = result.getAttachmentData();
        if (attachmentBytes == null || attachmentBytes.length == 0) {
            log.warn("[A] attachment data is null/empty");
            return;
        }

        LoginAttachment attachment = ProtobufPayloadCodec.decode(attachmentBytes, LoginAttachment.class);

        // 验证 B (login) 设置的字段
        log.info("[A] B fields: userId={}, username={}, token={}, loginTimeMillis={}, helloCount={}",
                attachment.getUserId(),
                attachment.getUsername(),
                attachment.getToken(),
                attachment.getLoginTimeMillis(),
                attachment.getHelloCount());

        boolean bFieldsOk = attachment.getUserId() > 0
                && "player1".equals(attachment.getUsername())
                && attachment.getToken() != null && attachment.getToken().startsWith("token-")
                && attachment.getLoginTimeMillis() > 0;
        log.info("[A] B 字段验证: {}", bFieldsOk ? "PASS" : "FAIL");

        // 验证 C (hello3) 设置的字段
        boolean cFieldsOk = "云生结海楼".equals(attachment.getCResult());
        log.info("[A] C cResult={}, C 字段验证: {}",
                attachment.getCResult(),
                cFieldsOk ? "PASS" : "FAIL");

        if (bFieldsOk && cFieldsOk) {
            log.info("[A] 链路验证全部通过: A→B→C attachment 传播正常");
        } else {
            log.error("[A] 链路验证失败: B={}, C={}", bFieldsOk, cFieldsOk);
        }
    }
}
