package cn.itsub.kynet.framework.action;

import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import org.agrona.DirectBuffer;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class ActionContext {
    private final ServiceNode serviceNode;
    private final long requestId;
    private final String traceId;
    private final long sessionId;
    private final long userId;
    private final int gatewayInstanceId;
    private volatile byte[] attachmentData;
    private DirectBuffer attachmentBuffer;
    private int attachmentOffset;
    private int attachmentLength;
    private volatile Object cachedAttachment;
    private volatile Class<?> cachedAttachmentType;
    private volatile boolean attachmentUpdated;

    public ActionContext(ServiceNode serviceNode, long requestId, String traceId,
                         long sessionId, long userId, byte[] attachmentData) {
        this(serviceNode, requestId, traceId, sessionId, userId, 0, attachmentData);
    }

    public ActionContext(ServiceNode serviceNode, long requestId, String traceId,
                         long sessionId, long userId, int gatewayInstanceId,
                         byte[] attachmentData) {
        this.serviceNode = serviceNode;
        this.requestId = requestId;
        this.traceId = traceId;
        this.sessionId = sessionId;
        this.userId = userId;
        this.gatewayInstanceId = gatewayInstanceId;
        this.attachmentData = attachmentData;
    }

    public ActionContext(ServiceNode serviceNode, long requestId, String traceId,
                         long sessionId, long userId, int gatewayInstanceId,
                         DirectBuffer attachmentBuffer, int attachmentOffset, int attachmentLength) {
        this.serviceNode = serviceNode;
        this.requestId = requestId;
        this.traceId = traceId;
        this.sessionId = sessionId;
        this.userId = userId;
        this.gatewayInstanceId = gatewayInstanceId;
        this.attachmentBuffer = attachmentLength > 0 ? attachmentBuffer : null;
        this.attachmentOffset = attachmentOffset;
        this.attachmentLength = Math.max(0, attachmentLength);
    }

    public ActionResult invoke(int cmd, int act, Object payload) {
        ActionResult result = serviceNode.invoke(cmd, act, payload, invokeOptions());
        applyInvokeResult(result);
        return result;
    }

    public CompletableFuture<ActionResult> invokeAsync(int cmd, int act, Object payload) {
        return serviceNode.invokeAsync(cmd, act, payload, invokeOptions())
                .thenApply(result -> {
                    applyInvokeResult(result);
                    return result;
                });
    }

    public void invokeOneWay(int cmd, int act, Object payload) {
        serviceNode.invokeOneWay(cmd, act, payload, invokeOptions());
    }

    public void pushToGateway(int cmd, int act, Object payload) {
        serviceNode.pushToGateway(sessionId, userId, cmd, act, payload, gatewayInstanceId);
    }

    public void broadcast(int msgId, Object payload) {
        broadcast(msgId, payload, (Collection<Long>) null);
    }

    public void broadcast(int msgId, Object payload, long playerId) {
        if (playerId <= 0) {
            throw new IllegalArgumentException("playerId must be positive");
        }
        serviceNode.pushToGateway(0L, playerId, ActionKey.cmdOf(msgId), ActionKey.actOf(msgId), payload, 0);
    }

    public void broadcast(int msgId, Object payload, Collection<Long> playerIds) {
        serviceNode.broadcast(msgId, payload, playerIds);
    }

    public void broadcastSelf(int msgId, Object payload) {
        if (sessionId <= 0 && userId <= 0) {
            throw new IllegalStateException("No sessionId or userId present in action context");
        }
        serviceNode.pushToGateway(sessionId, userId, ActionKey.cmdOf(msgId), ActionKey.actOf(msgId), payload, gatewayInstanceId);
    }

    /**
     * 断开当前玩家的连接（服务端权威，客户端无法拒绝）。走定向路由到承载网关，
     * UserHook.quit 照常触发。需要给玩家发下线提示时，先 pushToGateway/broadcastSelf 再调用本方法。
     */
    public void kickSelf() {
        kickSelf(null);
    }

    public void kickSelf(String reason) {
        if (sessionId <= 0) {
            throw new IllegalStateException("No sessionId present in action context");
        }
        serviceNode.kick(sessionId, 0L, 0L, gatewayInstanceId, reason);
    }

    /** 踢掉该用户当前的所有连接（封号/强制下线），包括当前会话（若属于该用户）。 */
    public void kickUser(long userId, String reason) {
        if (userId <= 0) {
            throw new IllegalArgumentException("userId must be positive");
        }
        serviceNode.kick(0L, userId, 0L, 0, reason);
    }

    /**
     * 顶号：踢掉该用户的旧连接，但保留当前会话。典型用法是登录成功后调用，
     * 把同一账号在其他设备上的旧连接挤下线。
     */
    public void kickOtherSessions(long userId, String reason) {
        if (userId <= 0) {
            throw new IllegalArgumentException("userId must be positive");
        }
        serviceNode.kick(0L, userId, sessionId, 0, reason);
    }

    public long getRequestId() {
        return requestId;
    }

    public String getTraceId() {
        return traceId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getUserId() {
        return userId;
    }

    public int getGatewayInstanceId() {
        return gatewayInstanceId;
    }

    public byte[] getAttachmentData() {
        return ensureAttachmentData();
    }

    public <T> T getAttachment(Class<T> targetType) {
        if (targetType == null) {
            throw new IllegalArgumentException("targetType must not be null");
        }
        byte[] localAttachment = ensureAttachmentData();
        if (localAttachment == null || localAttachment.length == 0) {
            return null;
        }
        Object cached = cachedAttachment;
        if (cached != null && targetType.equals(cachedAttachmentType)) {
            return targetType.cast(cached);
        }
        T decoded = ProtobufPayloadCodec.decode(localAttachment, targetType);
        cachedAttachment = decoded;
        cachedAttachmentType = targetType;
        return decoded;
    }

    public void updateAttachment(byte[] bytes) {
        attachmentData = bytes;
        attachmentBuffer = null;
        attachmentOffset = 0;
        attachmentLength = 0;
        cachedAttachment = null;
        cachedAttachmentType = null;
        attachmentUpdated = true;
    }

    public void updateAttachment(Object attachment) {
        updateAttachment(ProtobufPayloadCodec.encode(attachment));
    }

    public boolean isAttachmentUpdated() {
        return attachmentUpdated;
    }

    private InvokeOptions invokeOptions() {
        return InvokeOptions.builder()
                .gatewayInstanceId(gatewayInstanceId)
                .sessionId(sessionId)
                .userId(userId)
                .attachmentData(ensureAttachmentData())
                .timeout(Duration.ofMillis(300))
                .build();
    }

    private void applyInvokeResult(ActionResult result) {
        if (result == null) {
            return;
        }
        if (result.isAttachmentUpdated()) {
            updateAttachment(result.getAttachmentData());
        }
    }

    private byte[] ensureAttachmentData() {
        byte[] local = attachmentData;
        if (local != null) {
            return local;
        }
        DirectBuffer buffer = attachmentBuffer;
        if (buffer == null || attachmentLength <= 0) {
            return null;
        }
        byte[] copied = new byte[attachmentLength];
        buffer.getBytes(attachmentOffset, copied);
        attachmentData = copied;
        attachmentBuffer = null;
        attachmentOffset = 0;
        attachmentLength = 0;
        return copied;
    }
}
