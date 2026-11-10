package cn.itsub.kynet.framework.client;

import java.time.Duration;

public final class InvokeOptions {
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(300);
    private static final Duration DEFAULT_RETRY_BACKOFF = Duration.ofMillis(50);
    private static final int DEFAULT_MAX_RETRIES = 2;
    private static final InvokeOptions DEFAULT = builder().build();

    private final int targetInstanceId;
    private final int gatewayInstanceId;
    private final long sessionId;
    private final long userId;
    private final byte[] attachmentData;
    private final Duration timeout;
    private final boolean idempotent;
    private final int maxRetries;
    private final Duration retryBackoff;

    private InvokeOptions(Builder builder) {
        this.targetInstanceId = builder.targetInstanceId;
        this.gatewayInstanceId = builder.gatewayInstanceId;
        this.sessionId = builder.sessionId;
        this.userId = builder.userId;
        this.attachmentData = builder.attachmentData;
        this.timeout = builder.timeout == null ? DEFAULT_TIMEOUT : builder.timeout;
        this.idempotent = builder.idempotent;
        this.maxRetries = builder.maxRetries;
        this.retryBackoff = builder.retryBackoff == null ? DEFAULT_RETRY_BACKOFF : builder.retryBackoff;
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        if (retryBackoff.isNegative()) {
            throw new IllegalArgumentException("retryBackoff must not be negative");
        }
    }

    public static InvokeOptions defaults() {
        return DEFAULT;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getTargetInstanceId() {
        return targetInstanceId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public int getGatewayInstanceId() {
        return gatewayInstanceId;
    }

    public long getUserId() {
        return userId;
    }

    public byte[] getAttachmentData() {
        return attachmentData;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public boolean isIdempotent() {
        return idempotent;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Duration getRetryBackoff() {
        return retryBackoff;
    }

    public Builder toBuilder() {
        return builder()
                .targetInstanceId(targetInstanceId)
                .gatewayInstanceId(gatewayInstanceId)
                .sessionId(sessionId)
                .userId(userId)
                .attachmentData(attachmentData)
                .timeout(timeout)
                .idempotent(idempotent)
                .maxRetries(maxRetries)
                .retryBackoff(retryBackoff);
    }

    public static final class Builder {
        private int targetInstanceId;
        private int gatewayInstanceId;
        private long sessionId;
        private long userId;
        private byte[] attachmentData;
        private Duration timeout;
        private boolean idempotent;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private Duration retryBackoff;

        private Builder() {
        }

        public Builder targetInstanceId(int targetInstanceId) {
            this.targetInstanceId = targetInstanceId;
            return this;
        }

        public Builder sessionId(long sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder gatewayInstanceId(int gatewayInstanceId) {
            this.gatewayInstanceId = gatewayInstanceId;
            return this;
        }

        public Builder userId(long userId) {
            this.userId = userId;
            return this;
        }

        public Builder attachmentData(byte[] attachmentData) {
            this.attachmentData = attachmentData;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder idempotent(boolean idempotent) {
            this.idempotent = idempotent;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoff(Duration retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public InvokeOptions build() {
            return new InvokeOptions(this);
        }
    }
}
