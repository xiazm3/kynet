package cn.itsub.kynet.framework.gateway;

public record GatewayAuthenticationResult(boolean allowed, String errorCode, String errorMessage) {
    private static final GatewayAuthenticationResult ALLOWED = new GatewayAuthenticationResult(true, null, null);

    public static GatewayAuthenticationResult allow() {
        return ALLOWED;
    }

    public static GatewayAuthenticationResult deny(String errorCode, String errorMessage) {
        String resolvedCode = errorCode == null || errorCode.isBlank() ? "UNAUTHORIZED" : errorCode;
        String resolvedMessage = errorMessage == null || errorMessage.isBlank()
                ? "gateway authentication rejected"
                : errorMessage;
        return new GatewayAuthenticationResult(false, resolvedCode, resolvedMessage);
    }
}
