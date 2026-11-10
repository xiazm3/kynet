package cn.itsub.game.rpc.demo.action;

import cn.itsub.game.rpc.demo.message.RpcDemoRequest;
import cn.itsub.game.rpc.demo.message.RpcDemoResponse;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ActionController(cmd = 101)
public class ForwardEchoAction {

    RpcDemoResponse resp = new RpcDemoResponse("OK", "xxx");

    @ActionMethod(act = 1)
    public RpcDemoResponse forwardOnly(RpcDemoRequest request, ActionContext context) {
        String detail = request == null ? "echo:null" : "echo:" + request.getMessage();
        log.info(detail);
        return resp;
    }
}
