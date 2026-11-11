package cn.itsub.game.login.action;

import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import game.message.SignResultMessage;
import game.message.SignTokenMessage;

import java.util.List;

/**
 * 代码生成示例：客户端通过 kynet-codegen 依据本类生成各语言代理类。
 * 覆盖三种载荷形态：消息 → 消息、int → int（PrimitiveValue 信封）、String → List&lt;String&gt;（ListValue 信封）。
 * cmd=2 与 LoginAction(cmd=1) 区分。
 */
@ActionController(2)
public final class SignAction {

    /** 签到：返回签到结果。 */
    @ActionMethod(1)
    public SignResultMessage sign(SignTokenMessage token) {
        SignResultMessage result = new SignResultMessage();
        result.setDay(3);
        result.setBonus("签到成功：" + token.getTicket());
        return result;
    }

    /** 下一个签到日序号。 */
    @ActionMethod(2)
    public int nextDay(int day) {
        return day + 1;
    }

    /** 按渠道返回签到奖励文案。 */
    @ActionMethod(3)
    public List<String> rewards(String channel) {
        return List.of(channel + "-首签礼包", "连续签到+500金币");
    }
}
