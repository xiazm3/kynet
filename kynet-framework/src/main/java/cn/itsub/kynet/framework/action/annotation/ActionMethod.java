package cn.itsub.kynet.framework.action.annotation;

import cn.itsub.kynet.framework.action.ActionExecutionMode;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ActionMethod {
    int act();

    ActionExecutionMode mode() default ActionExecutionMode.ASYNC;
}
