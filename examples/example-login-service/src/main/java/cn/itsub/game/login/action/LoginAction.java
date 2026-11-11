package cn.itsub.game.login.action;

import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import cn.itsub.kynet.framework.gateway.PlayerBroadcaster;
import com.google.protobuf.StringValue;
import game.message.LoginMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

@Slf4j
@ActionController(1)
public class LoginAction {

    @ActionMethod(1)
    public ActionResult login(LoginMessage loginMessage, ActionContext context) {
        log.info("login request {}", loginMessage);
        String username = loginMessage == null ? null : loginMessage.getUsername();
        String password = loginMessage == null ? null : loginMessage.getPassword();
        log.info("login request username={}, password={}", username, password);
        if (username == null || username.isBlank() || !"123456".equals(password)) {
            return ActionResult.error("LOGIN_FAILED", "username or password invalid");
        }

        long userId = 10001;

        // 登录状态只保留 loginId，业务上下文统一放 attachment，跟随后续请求传播。
        StpUtil.login(userId);
        StpUtil.checkLogin();

        log.info("login request username={}, sessionId={}", username, context.getSessionId());

        String token = "token-" + username;
        loginMessage.setToken(token);

        // 示例：登录成功后初始化 attachment，后续整条 action 调用链都会自动携带。
        LoginAttachment attachment = new LoginAttachment();
        attachment.setUserId(userId);
        attachment.setUsername(username);
        attachment.setToken(token);
        attachment.setLoginTimeMillis(System.currentTimeMillis());
        attachment.setHelloCount(0);
        context.updateAttachment(attachment);

        // 顶号：服务端权威踢掉同一账号在其他设备上的旧连接，保留当前会话。
        // 旧连接会触发 UserHook.quit，客户端表现为连接被服务端关闭。
        context.kickOtherSessions(userId, "账号在其他设备登录");

        if (context.getSessionId() > 0 || context.getUserId() > 0) {
            PlayerBroadcaster broadcaster = new PlayerBroadcaster(context);
            broadcaster.broadcast(5, 6, "welcome " + username);
        }

        ActionResult result = context.invoke(1, 3, loginMessage);
        log.info("login result {}", result);

        LoginAttachment attachmentAfterC = context.getAttachment(LoginAttachment.class);
        log.info("[B verify] attachment after C call: userId={}, cResult={}",
                attachmentAfterC != null ? attachmentAfterC.getUserId() : 0,
                attachmentAfterC != null ? attachmentAfterC.getCResult() : "null");

        return ActionResult.success(loginMessage);
    }

    @ActionMethod(2)
    public ActionResult hello(ActionContext context) {
        StpUtil.checkLogin();
        LoginAttachment attachment = context.getAttachment(LoginAttachment.class);
        if (attachment == null) {
            String text = "hello"
                    + ", sessionId=" + context.getSessionId()
                    + ", attachment=null";
            return ActionResult.success(text);
        }

        attachment.setHelloCount(attachment.getHelloCount() + 1);
        context.updateAttachment(attachment);

        String text = "hello"
                + ", sessionId=" + context.getSessionId()
                + ", attachment.userId=" + attachment.getUserId()
                + ", attachment.username=" + attachment.getUsername()
                + ", attachment.token=" + attachment.getToken()
                + ", helloCount=" + attachment.getHelloCount();
        return ActionResult.success(text);
    }

    @ActionMethod(4)
    public ActionResult logout(ActionContext context) {
        StpUtil.checkLogin();
        Long loginId = StpUtil.getLoginIdAsLong();
        if (loginId != null && loginId > 0) {
            StpUtil.logout(loginId);
        } else {
            StpUtil.logout();
        }
        return ActionResult.success("logout success");
    }


    @ActionMethod(3)
    public ActionResult hello3(LoginMessage loginMessage, ActionContext context) {
        LoginAttachment attachment = context.getAttachment(LoginAttachment.class);
        log.info("hello3 username={}", attachment != null ? attachment.getUsername() : "null");
        if (attachment != null) {
            attachment.setCResult("云生结海楼");
            context.updateAttachment(attachment);
        }
        return ActionResult.success("云生结海楼", context.getAttachmentData());
    }

    @ActionMethod(5)
    public ActionResult testBroadcast(ActionContext context) {
        LoginMessage loginMessage = new LoginMessage();
        loginMessage.setToken("hello");

        int msgType = 200001;

        // 1. 原生 google.protobuf 消息，直接沿用原 msgId
        context.broadcast(msgType, StringValue.of("hello"));
        // 2. 常见简单类型，内部自动包装成 google wrapper 再序列化
        context.broadcast(msgType, "hello");
        // 3. 带 @ProtobufClass 的 POJO，走 jprotobuf 编码
        context.broadcast(msgType, loginMessage);

        // 广播消息给指定用户
        context.broadcast(msgType, "hello1", 10001);
        // 广播消息给多个指定用户。broker 仍会范围投递给全部 gateway，
        // 每个 gateway 再按本地 session/userId 过滤出命中的玩家。
        List<Long> userIds = Arrays.asList(10001L, 10002L, 10003L);
        context.broadcast(msgType, "hello2", userIds);
        // 广播给自己。当前 action 已知来源 gateway，可在 broker 层精准投递。
        context.broadcastSelf(msgType, "hello3");
        log.info("broadcast success");

        //客户端订阅消息 Kynet.NetworkManager.Instance.AddListener(200001, OnGetMsg);

        return ActionResult.success("logout success");
    }
}
