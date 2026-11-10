package cn.itsub.game.login.action;

import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionContextHolder;
import cn.itsub.kynet.framework.action.BaseAttachment;

public final class StpUtil {

    private StpUtil() {
    }

    public static void login(long loginId) {
        if (loginId <= 0) {
            throw new IllegalArgumentException("loginId must be positive");
        }
        ActionContext context = currentContext();
        BaseAttachment attachment = context.getAttachment(BaseAttachment.class);
        if (attachment == null) {
            attachment = new BaseAttachment();
        }
        attachment.setUserId(loginId);
        context.updateAttachment(attachment);
    }

    public static boolean isLogin() {
        Long loginId = getLoginIdAsLong();
        return loginId != null && loginId > 0;
    }

    public static void checkLogin() {
        if (!isLogin()) {
            clearLoginState(currentContext());
            throw new NotLoginException("authentication required");
        }
    }

    public static void logout() {
        clearLoginState(currentContext());
    }

    public static void logout(long loginId) {
        if (loginId <= 0) {
            throw new IllegalArgumentException("loginId must be positive");
        }
        Long currentLoginId = getLoginIdAsLong();
        if (Long.valueOf(loginId).equals(currentLoginId)) {
            clearLoginState(currentContext());
        }
    }

    public static Long getLoginIdAsLong() {
        BaseAttachment attachment = currentContext().getAttachment(BaseAttachment.class);
        if (attachment == null || attachment.getUserId() <= 0) {
            return null;
        }
        return attachment.getUserId();
    }

    private static ActionContext currentContext() {
        return ActionContextHolder.currentRequired();
    }

    private static void clearLoginState(ActionContext context) {
        BaseAttachment attachment = context.getAttachment(BaseAttachment.class);
        if (attachment != null) {
            attachment.setUserId(0);
            context.updateAttachment(attachment);
        }
    }
}
