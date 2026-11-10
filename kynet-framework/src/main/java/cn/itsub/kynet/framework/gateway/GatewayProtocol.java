package cn.itsub.kynet.framework.gateway;

import java.util.Locale;

public enum GatewayProtocol {
    TCP,
    WS,
    UDP,
    KCP;

    public static GatewayProtocol fromScheme(String scheme) {
        if (scheme == null || scheme.isBlank()) {
            throw new IllegalArgumentException("Gateway endpoint scheme is required");
        }
        return switch (scheme.toLowerCase(Locale.ROOT)) {
            case "tcp", "tcps" -> TCP;
            case "ws", "wss" -> WS;
            case "udp" -> UDP;
            case "kcp" -> KCP;
            default -> throw new IllegalArgumentException("Unsupported gateway protocol: " + scheme);
        };
    }
}
