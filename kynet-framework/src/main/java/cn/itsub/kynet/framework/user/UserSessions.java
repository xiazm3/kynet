package cn.itsub.kynet.framework.user;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UserSessions {
    private final Map<Long, UserSession> sessions = new ConcurrentHashMap<>();

    public void add(UserSession session) {
        sessions.put(session.getSessionId(), session);
    }

    public UserSession remove(long sessionId) {
        return sessions.remove(sessionId);
    }

    public UserSession get(long sessionId) {
        return sessions.get(sessionId);
    }

    public long countOnline() {
        return sessions.size();
    }

    public Collection<UserSession> values() {
        return sessions.values();
    }
}
