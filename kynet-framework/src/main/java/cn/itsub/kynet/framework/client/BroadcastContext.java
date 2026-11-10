package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.util.ActionKey;

import java.util.Collection;
import java.util.Objects;

public final class BroadcastContext {
    private final ServiceNode serviceNode;

    BroadcastContext(ServiceNode serviceNode) {
        this.serviceNode = Objects.requireNonNull(serviceNode, "serviceNode");
    }

    public void broadcast(int cmdInfo, Object payload) {
        broadcast(cmdInfo, payload, null);
    }

    public void broadcast(int msgId, Object payload, Collection<Long> playerIds) {
        serviceNode.broadcast(msgId, payload, playerIds);
    }

    public void broadcast(int cmdInfo, Object payload, long userId) {
        if (userId <= 0) {
            throw new IllegalArgumentException("userId must be positive");
        }
        serviceNode.pushToGateway(0L, userId, ActionKey.cmdOf(cmdInfo), ActionKey.actOf(cmdInfo), payload, 0);
    }

}
