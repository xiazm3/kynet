package cn.itsub.kynet.framework.action.annotation;

/** {@link ActionController#value()} / {@link ActionMethod#value()} 的统一校验入口。 */
public final class ActionAnnotations {
    private ActionAnnotations() {
    }

    /** 解析控制器 cmd；缺省或非法时抛 {@link IllegalArgumentException}。 */
    public static int cmd(ActionController controller) {
        int value = controller.value();
        if (value <= 0) {
            throw new IllegalArgumentException("@ActionController 缺少 value（如 @ActionController(2)）");
        }
        return value;
    }

    /** 解析方法 act；缺省或非法时抛 {@link IllegalArgumentException}。 */
    public static int act(ActionMethod method) {
        int value = method.value();
        if (value <= 0) {
            throw new IllegalArgumentException("@ActionMethod 缺少 value（如 @ActionMethod(1)）");
        }
        return value;
    }
}
