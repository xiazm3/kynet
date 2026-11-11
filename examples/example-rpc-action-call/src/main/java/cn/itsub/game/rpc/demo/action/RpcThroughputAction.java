package cn.itsub.game.rpc.demo.action;

import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionExecutionMode;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;

@ActionController(99)
public class RpcThroughputAction {

    @ActionMethod(value = 1, mode = ActionExecutionMode.INLINE)
    public byte[] ping(ActionContext context) {
        return null;
    }
}
