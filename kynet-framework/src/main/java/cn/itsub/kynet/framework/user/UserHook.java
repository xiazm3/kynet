package cn.itsub.kynet.framework.user;

public interface UserHook {
    void into(UserSession userSession);

    void quit(UserSession userSession);

    /**
     * 登录态绑定到该连接：业务 Action 返回的 attachment.userId 被网关写回会话时触发。
     * {@code previousUserId} 为该连接此前绑定的用户（首次登录为 0；换号登录为旧 userId）。
     */
    default void bind(UserSession userSession, long previousUserId) {
    }
}
