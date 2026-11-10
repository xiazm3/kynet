package cn.itsub.kynet.framework.action;

import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import org.agrona.DirectBuffer;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class ActionInvocation {
    private enum InvocationMode {
        NO_ARGS,
        CONTEXT_ONLY,
        PAYLOAD_ONLY,
        CONTEXT_PAYLOAD,
        PAYLOAD_CONTEXT
    }

    private final Object bean;
    private final MethodHandle invocationHandle;
    private final int action;
    private final Class<?> payloadType;
    private final InvocationMode invocationMode;
    private final ActionExecutionMode executionMode;
    private final boolean decodePayload;
    private final boolean byteArrayPayload;

    public ActionInvocation(Object bean, Method method, int action, Class<?> payloadType) {
        this(bean, method, action, payloadType, ActionExecutionMode.ASYNC);
    }

    public ActionInvocation(Object bean, Method method, int action, Class<?> payloadType,
                            ActionExecutionMode executionMode) {
        this.bean = bean;
        MethodHandle methodHandle = resolveMethodHandle(method);
        this.action = action;
        this.payloadType = payloadType;
        this.invocationMode = resolveInvocationMode(method);
        this.invocationHandle = adaptInvocationHandle(method, methodHandle, invocationMode);
        this.executionMode = executionMode == null ? ActionExecutionMode.ASYNC : executionMode;
        this.decodePayload = switch (invocationMode) {
            case PAYLOAD_ONLY, CONTEXT_PAYLOAD, PAYLOAD_CONTEXT -> payloadType != null;
            default -> false;
        };
        this.byteArrayPayload = decodePayload && byte[].class.equals(payloadType);
    }

    int action() {
        return action;
    }

    public ActionExecutionMode executionMode() {
        return executionMode;
    }

    public boolean expectsByteArrayPayload() {
        return byteArrayPayload;
    }

    public Object invoke(byte[] payload, ActionContext context) throws Exception {
        Object converted = decodePayload ? decodePayload(payload) : null;
        return invokeConverted(converted, context);
    }

    public Object invoke(DirectBuffer payloadBuffer, int payloadOffset, int payloadLength, ActionContext context) throws Exception {
        Object converted = decodePayload
                ? ProtobufPayloadCodec.decode(payloadBuffer, payloadOffset, payloadLength, payloadType)
                : null;
        return invokeConverted(converted, context);
    }

    private Object decodePayload(byte[] payload) {
        if (byteArrayPayload) {
            return payload;
        }
        return ProtobufPayloadCodec.decode(payload, payloadType);
    }

    private Object invokeConverted(Object converted, ActionContext context) throws Exception {
        ActionContext previous = ActionContextHolder.set(context);
        try {
            return switch (invocationMode) {
                case NO_ARGS -> invokeNoArgs();
                case CONTEXT_ONLY -> invokeContext(context);
                case PAYLOAD_ONLY -> invokePayload(converted);
                case CONTEXT_PAYLOAD -> invokeContextPayload(context, converted);
                case PAYLOAD_CONTEXT -> invokePayloadContext(converted, context);
            };
        } catch (Throwable throwable) {
            if (throwable instanceof Exception exception) {
                throw exception;
            }
            if (throwable instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("Action invocation failed", throwable);
        } finally {
            ActionContextHolder.restore(previous);
        }
    }

    private Object invokeNoArgs() throws Throwable {
        return (Object) invocationHandle.invokeExact();
    }

    private Object invokeContext(ActionContext context) throws Throwable {
        return (Object) invocationHandle.invokeExact(context);
    }

    private Object invokePayload(Object payload) throws Throwable {
        return (Object) invocationHandle.invokeExact(payload);
    }

    private Object invokeContextPayload(ActionContext context, Object payload) throws Throwable {
        return (Object) invocationHandle.invokeExact(context, payload);
    }

    private Object invokePayloadContext(Object payload, ActionContext context) throws Throwable {
        return (Object) invocationHandle.invokeExact(payload, context);
    }

    private MethodHandle resolveMethodHandle(Method method) {
        try {
            MethodHandle handle = MethodHandles.lookup().unreflect(method);
            return Modifier.isStatic(method.getModifiers()) ? handle : handle.bindTo(bean);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Failed to access action method " + method, e);
        }
    }

    private MethodHandle adaptInvocationHandle(Method method, MethodHandle handle, InvocationMode mode) {
        try {
            return switch (mode) {
                case NO_ARGS -> handle.asType(MethodType.methodType(Object.class));
                case CONTEXT_ONLY -> handle.asType(MethodType.methodType(Object.class, ActionContext.class));
                case PAYLOAD_ONLY -> handle.asType(MethodType.methodType(Object.class, Object.class));
                case CONTEXT_PAYLOAD -> handle.asType(MethodType.methodType(Object.class, ActionContext.class, Object.class));
                case PAYLOAD_CONTEXT -> handle.asType(MethodType.methodType(Object.class, Object.class, ActionContext.class));
            };
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to adapt action method handle " + method, e);
        }
    }

    private InvocationMode resolveInvocationMode(Method method) {
        int parameterCount = method.getParameterCount();
        if (parameterCount == 0) {
            return InvocationMode.NO_ARGS;
        }
        if (parameterCount == 1) {
            return ActionContext.class.equals(method.getParameterTypes()[0])
                    ? InvocationMode.CONTEXT_ONLY
                    : InvocationMode.PAYLOAD_ONLY;
        }
        Class<?> firstParameter = method.getParameterTypes()[0];
        return ActionContext.class.equals(firstParameter)
                ? InvocationMode.CONTEXT_PAYLOAD
                : InvocationMode.PAYLOAD_CONTEXT;
    }
}
