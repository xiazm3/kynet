package cn.itsub.kynet.framework.action;

public final class ActionContextHolder {
    private static final ThreadLocal<ActionContext> CONTEXT = new ThreadLocal<>();

    private ActionContextHolder() {
    }

    public static ActionContext current() {
        return CONTEXT.get();
    }

    public static ActionContext currentRequired() {
        ActionContext context = current();
        if (context == null) {
            throw new IllegalStateException("No ActionContext bound to current thread");
        }
        return context;
    }

    static ActionContext set(ActionContext context) {
        ActionContext previous = CONTEXT.get();
        if (context == null) {
            CONTEXT.remove();
        } else {
            CONTEXT.set(context);
        }
        return previous;
    }

    static void restore(ActionContext previous) {
        if (previous == null) {
            CONTEXT.remove();
        } else {
            CONTEXT.set(previous);
        }
    }
}
