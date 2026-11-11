package cn.itsub.kynet.framework.action;

import cn.itsub.kynet.framework.action.annotation.ActionAnnotations;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActionAnnotationsTest {

    @ActionController(7)
    static class Shorthand {
        @ActionMethod(3)
        public void run() {
        }

        @ActionMethod(value = 4, mode = ActionExecutionMode.INLINE)
        public void inlineRun() {
        }
    }

    @Test
    void valueIsResolvedAsRoutingNumber() throws Exception {
        assertEquals(7, ActionAnnotations.cmd(Shorthand.class.getAnnotation(ActionController.class)));
        assertEquals(3, actOf(Shorthand.class, "run"));
        assertEquals(4, actOf(Shorthand.class, "inlineRun"));
    }

    @Test
    void valueIsMandatoryAtCompileTime() {
        // value() 无默认值：@ActionController / @ActionMethod 缺省直接编译失败，不再有运行时缺省路径。
        for (Method member : ActionController.class.getDeclaredMethods()) {
            assertEquals("value", member.getName());
            assertTrue(member.getDefaultValue() == null, member + " 应无默认值以强制编译期检查");
        }
        for (Method member : ActionMethod.class.getDeclaredMethods()) {
            if (member.getName().equals("value")) {
                assertTrue(member.getDefaultValue() == null);
            } else {
                assertEquals("mode", member.getName());
            }
        }
    }

    @Test
    void nonPositiveValueIsRejectedDefensively() {
        // 注解值是编译期常量，正常代码构造不出 <=0；校验仅作反射/字节码注入的防御。
        ActionController hostile = new ActionController() {
            @Override public int value() { return 0; }
            @Override public Class<? extends Annotation> annotationType() { return ActionController.class; }
        };
        assertThrows(IllegalArgumentException.class, () -> ActionAnnotations.cmd(hostile));
    }

    private int actOf(Class<?> type, String method) throws Exception {
        return ActionAnnotations.act(type.getDeclaredMethod(method).getAnnotation(ActionMethod.class));
    }
}
