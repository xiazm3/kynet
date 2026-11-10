package cn.itsub.kynet.common.util;

public final class ActionKey {
    private ActionKey() {
    }

    public static int of(int cmd, int act) {
        if (cmd < 0 || cmd > 0xFFFF) {
            throw new IllegalArgumentException("cmd must be between 0 and 65535");
        }
        if (act < 0 || act > 0xFFFF) {
            throw new IllegalArgumentException("act must be between 0 and 65535");
        }
        return (cmd << 16) | (act & 0xFFFF);
    }

    public static int cmdOf(int action) {
        return (action >>> 16) & 0xFFFF;
    }

    public static int actOf(int action) {
        return action & 0xFFFF;
    }
}
