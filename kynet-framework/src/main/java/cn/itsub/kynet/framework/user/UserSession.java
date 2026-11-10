package cn.itsub.kynet.framework.user;

import java.time.Instant;

public class UserSession {
    private final long sessionId;
    private final int gatewayInstanceId;
    private final String remoteAddress;
    private volatile long userId;
    private volatile byte[] attachmentData;
    private final String protocolType;
    private final Instant connectTime;
    private volatile Instant lastActiveTime;
    private volatile boolean authenticated;

    public UserSession(long sessionId, String protocolType) {
        this(sessionId, protocolType, 0, null);
    }

    public UserSession(long sessionId, String protocolType, int gatewayInstanceId, String remoteAddress) {
        this.sessionId = sessionId;
        this.gatewayInstanceId = gatewayInstanceId;
        this.remoteAddress = remoteAddress;
        this.protocolType = protocolType;
        this.connectTime = Instant.now();
        this.lastActiveTime = this.connectTime;
    }

    public long getSessionId() {
        return sessionId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public byte[] getAttachmentData() {
        return attachmentData;
    }

    public void setAttachmentData(byte[] attachmentData) {
        this.attachmentData = attachmentData;
    }

    public String getProtocolType() {
        return protocolType;
    }

    public int getGatewayInstanceId() {
        return gatewayInstanceId;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public Instant getConnectTime() {
        return connectTime;
    }

    public Instant getLastActiveTime() {
        return lastActiveTime;
    }

    public void touch() {
        this.lastActiveTime = Instant.now();
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }
}
