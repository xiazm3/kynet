package game.external;

import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.ServiceNodeAware;
import cn.itsub.kynet.framework.user.UserHook;
import cn.itsub.kynet.framework.user.UserSession;
import cn.itsub.kynet.framework.user.UserSessions;
import cn.itsub.kynet.framework.user.UserSessionsAware;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExternalUserHook implements UserHook, UserSessionsAware, ServiceNodeAware {
    private UserSessions userSessions;
    private ServiceNode brokerClient;

    @Override
    public void into(UserSession userSession) {
        log.info("gateway into: {} online={} brokerServiceId={}", userSession.getSessionId(),
                userSessions == null ? "?" : userSessions.countOnline(),
                brokerClient == null ? "?" : brokerClient.getServiceId());
    }

    @Override
    public void quit(UserSession userSession) {
        log.info("gateway quit: {} online={}", userSession.getSessionId(),
                userSessions == null ? "?" : userSessions.countOnline());
    }

    @Override
    public void setUserSessions(UserSessions userSessions) {
        this.userSessions = userSessions;
    }

    @Override
    public void setServiceNode(ServiceNode brokerClient) {
        this.brokerClient = brokerClient;
    }
}
