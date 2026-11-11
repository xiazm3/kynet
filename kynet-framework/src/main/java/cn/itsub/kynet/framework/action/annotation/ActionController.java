package cn.itsub.kynet.framework.action.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** action 控制器注解：value 即网关路由的 cmd，取值 1..65535。 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ActionController {
    /** 控制器 cmd，如 @ActionController(2)。 */
    int value();
}
