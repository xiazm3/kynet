package cn.itsub.kynet.framework.gateway;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AccessControl {
    private final Set<Integer> allowAllActions = ConcurrentHashMap.newKeySet();
    private final Set<Integer> denyAllActions = ConcurrentHashMap.newKeySet();
    private final Set<Integer> authRequiredActions = ConcurrentHashMap.newKeySet();

    public void allow(int cmd, int act) {
        allowAllActions.add((cmd << 16) | (act & 0xFFFF));
    }

    public void deny(int cmd) {
        denyAllActions.add(cmd);
    }

    public void auth(int cmd, int act) {
        authRequiredActions.add((cmd << 16) | (act & 0xFFFF));
    }

    public boolean requiresAuth(int action) {
        return authRequiredActions.contains(action);
    }

    public boolean isDenied(int cmd) {
        return denyAllActions.contains(cmd);
    }

    public boolean isAllowed(int action) {
        return allowAllActions.contains(action);
    }
}
