package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.framework.user.UserSession;

public record GatewayRequestContext(
        UserSession session,
        GatewayPacket packet,
        String protocol,
        String remoteAddress,
        String remoteIp
) {
}
