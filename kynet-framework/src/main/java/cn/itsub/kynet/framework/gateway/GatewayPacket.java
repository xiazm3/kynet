package cn.itsub.kynet.framework.gateway;

public record GatewayPacket(
        GatewayPacketType type,
        long requestId,
        int cmd,
        int act,
        boolean oneWay,
        long timeoutMillis,
        long sessionId,
        long userId,
        String errorCode,
        String errorMessage,
        boolean attachmentUpdated,
        byte[] attachmentData,
        byte[] payload
) {
    public static GatewayPacket request(long requestId, int cmd, int act, boolean oneWay, long timeoutMillis,
                                        long sessionId, long userId, byte[] attachmentData, byte[] payload) {
        return new GatewayPacket(GatewayPacketType.REQUEST, requestId, cmd, act, oneWay, timeoutMillis,
                sessionId, userId, null, null, false, attachmentData, payload);
    }

    public static GatewayPacket response(long requestId, int cmd, int act, long sessionId, long userId,
                                         boolean attachmentUpdated, byte[] attachmentData, byte[] payload) {
        return new GatewayPacket(GatewayPacketType.RESPONSE, requestId, cmd, act, false, 0L,
                sessionId, userId, null, null, attachmentUpdated, attachmentData, payload);
    }

    public static GatewayPacket error(long requestId, int cmd, int act, long sessionId, long userId,
                                      String errorCode, String errorMessage,
                                      boolean attachmentUpdated, byte[] attachmentData) {
        return new GatewayPacket(GatewayPacketType.RESPONSE, requestId, cmd, act, false, 0L,
                sessionId, userId, errorCode, errorMessage, attachmentUpdated, attachmentData, null);
    }

    public static GatewayPacket push(int cmd, int act, long sessionId, long userId, byte[] payload) {
        return new GatewayPacket(GatewayPacketType.PUSH, 0L, cmd, act, false, 0L,
                sessionId, userId, null, null, false, null, payload);
    }

    public static GatewayPacket pong(long requestId, long sessionId, long userId, byte[] attachmentData) {
        return new GatewayPacket(GatewayPacketType.PONG, requestId, 0, 0, false, 0L,
                sessionId, userId, null, null, true, attachmentData, null);
    }
}
