package cn.itsub.kynet.framework.action;

import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.framework.action.annotation.ActionAnnotations;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import cn.itsub.kynet.framework.client.ServiceNode;

import java.lang.reflect.Method;

public class ActionBootstrap {
    private final ServiceNode serviceNode;

    public ActionBootstrap(ServiceNode serviceNode) {
        this.serviceNode = serviceNode;
    }

    public void registerAction(Class<?> controllerType) {
        try {
            Object bean = controllerType.getDeclaredConstructor().newInstance();
            registerAction(bean);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate controller " + controllerType.getName(), e);
        }
    }

    public void registerAction(Object controller) {
        Class<?> controllerType = controller.getClass();
        ActionController actionController = controllerType.getAnnotation(ActionController.class);
        if (actionController == null) {
            throw new IllegalArgumentException(controllerType.getName() + " is missing @ActionController");
        }
        int cmd = ActionAnnotations.cmd(actionController);
        for (Method method : controllerType.getDeclaredMethods()) {
            ActionMethod actionMethod = method.getAnnotation(ActionMethod.class);
            if (actionMethod == null) {
                continue;
            }
            method.setAccessible(true);
            int action = ActionKey.of(cmd, ActionAnnotations.act(actionMethod));
            Class<?> payloadType = resolvePayloadType(method);
            serviceNode.registerActionHandler(action,
                    new ActionInvocation(controller, method, action, payloadType, actionMethod.mode()));
        }
    }

    private Class<?> resolvePayloadType(Method method) {
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (!ActionContext.class.equals(parameterType)) {
                return parameterType;
            }
        }
        return null;
    }
}
