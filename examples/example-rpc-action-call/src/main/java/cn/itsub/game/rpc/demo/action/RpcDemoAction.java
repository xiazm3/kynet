package cn.itsub.game.rpc.demo.action;

import cn.itsub.game.rpc.demo.message.RpcDemoRequest;
import cn.itsub.game.rpc.demo.message.RpcDemoResponse;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionExecutionMode;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ActionController(10)
public class RpcDemoAction {

    private RpcDemoResponse RESP_SUCCESS = new RpcDemoResponse("SUCCESS", "HELLO");

    @ActionMethod(value = 1, mode = ActionExecutionMode.ASYNC)
    public int success(RpcDemoRequest request, ActionContext context) {
        if (log.isDebugEnabled()) {
            log.debug("act=1 success call, requestId={}, sessionId={}, traceId={}",
                    context.getRequestId(), context.getSessionId(), context.getTraceId());
        }
        return 0;
    }

}
