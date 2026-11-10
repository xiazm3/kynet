package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.framework.user.UserSession;

public interface GatewayConnection {
    UserSession userSession();

    String protocolType();

    String remoteAddress();

    default String remoteIp() {
        return remoteAddress();
    }

    boolean isOpen();

    void send(GatewayPacket packet);

    void close();
}
