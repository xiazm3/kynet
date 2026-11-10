package cn.itsub.game.rpc.demo.action;

import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionExecutionMode;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;

@ActionController(cmd = 99)
public class RpcThroughputAction {

    @ActionMethod(act = 1, mode = ActionExecutionMode.INLINE)
    public byte[] ping(ActionContext context) {
        return null;
    }
}
