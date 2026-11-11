package cn.itsub.kynet.codegen.fixture;

import cn.itsub.kynet.codegen.fixture.FixtureMessages.FixtureLoginVerifyMessage;
import cn.itsub.kynet.codegen.fixture.FixtureMessages.FixtureUserMessage;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;

import java.util.List;

/** 扫描器/渲染器测试的控制器夹具，覆盖：消息/基础类型/列表返回/无返回/仅 Context。 */
@ActionController(9)
public class FixtureMyAction {

    @ActionMethod(1)
    public FixtureUserMessage loginVerify(FixtureLoginVerifyMessage verifyMessage) {
        return new FixtureUserMessage();
    }

    @ActionMethod(2)
    public int intValue(int value) {
        return value + 1;
    }

    @ActionMethod(3)
    public List<String> listString(String value) {
        return List.of(value);
    }

    @ActionMethod(4)
    public List<FixtureUserMessage> listUsers(String owner) {
        return List.of();
    }

    @ActionMethod(5)
    public void fireAndForget(FixtureLoginVerifyMessage message) {
        // 无返回值
    }

    @ActionMethod(6)
    public FixtureUserMessage contextOnly(ActionContext context) {
        return new FixtureUserMessage();
    }

    @ActionMethod(7)
    public FixtureMessages.FixtureMixed mixedOrder(FixtureMessages.FixtureMixed mixed) {
        return mixed;
    }
}
