package cn.itsub.kynet.framework.action.annotation;

import cn.itsub.kynet.framework.action.ActionExecutionMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** action 方法注解：value 即网关路由的 act，取值 1..65535。 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ActionMethod {
    /** 方法 act，如 @ActionMethod(1)。 */
    int value();

    ActionExecutionMode mode() default ActionExecutionMode.ASYNC;
}
