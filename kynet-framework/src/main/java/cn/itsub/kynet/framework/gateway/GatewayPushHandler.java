package cn.itsub.kynet.framework.gateway;

import cn.itsub.kynet.common.protocol.GatewayPushMessage;

@FunctionalInterface
public interface GatewayPushHandler {
    void onPush(GatewayPushMessage pushMessage);
}
