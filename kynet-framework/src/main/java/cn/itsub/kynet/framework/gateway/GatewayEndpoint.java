package cn.itsub.kynet.framework.gateway;

import java.net.URI;
import java.util.Locale;

public record GatewayEndpoint(
        String raw,
        GatewayProtocol protocol,
        String host,
        int port,
        String path,
        boolean secure
) {
    public static GatewayEndpoint parse(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("Gateway endpoint is required");
        }
        URI uri = URI.create(endpoint);
        String rawScheme = uri.getScheme();
        if (rawScheme == null || rawScheme.isBlank()) {
            throw new IllegalArgumentException("Gateway endpoint scheme is required: " + endpoint);
        }
        String host = uri.getHost();
        int port = uri.getPort();
        if (host == null || host.isBlank() || port <= 0) {
            throw new IllegalArgumentException("Gateway endpoint must include host and port: " + endpoint);
        }
        String path = uri.getPath();
        if (path == null || path.isBlank()) {
            path = "/";
        }
        String scheme = rawScheme.toLowerCase(Locale.ROOT);
        return new GatewayEndpoint(endpoint, GatewayProtocol.fromScheme(scheme), host, port, path,
                "wss".equals(scheme) || "tcps".equals(scheme));
    }
}
