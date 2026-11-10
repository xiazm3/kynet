package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.framework.action.ActionContext;

public class PlayerBroadcaster {
    private final ActionContext context;

    public PlayerBroadcaster(ActionContext context) {
        this.context = context;
    }

    public void broadcast(int cmd, int act, Object payload) {
        if (context.getSessionId() <= 0 && context.getUserId() <= 0) {
            throw new IllegalStateException("No sessionId or userId present in action context");
        }
        context.pushToGateway(cmd, act, payload);
    }
}
