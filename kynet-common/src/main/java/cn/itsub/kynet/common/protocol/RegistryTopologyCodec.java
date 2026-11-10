package cn.itsub.kynet.common.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class RegistryTopologyCodec {
    private RegistryTopologyCodec() {
    }

    public static byte[] encode(Collection<Endpoint> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return new byte[0];
        }
        StringBuilder builder = new StringBuilder();
        for (Endpoint endpoint : endpoints) {
            if (endpoint == null || endpoint.host() == null || endpoint.host().isBlank() || endpoint.port() <= 0) {
                continue;
            }
            if (!builder.isEmpty()) {
                builder.append(';');
            }
            builder.append(nullToEmpty(endpoint.nodeId()))
                    .append('|')
                    .append(endpoint.host())
                    .append('|')
                    .append(endpoint.port())
                    .append('|')
                    .append(nullToEmpty(endpoint.state()));
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static List<Endpoint> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return List.of();
        }
        String text = new String(payload, StandardCharsets.UTF_8);
        if (text.isBlank()) {
            return List.of();
        }
        List<Endpoint> endpoints = new ArrayList<>();
        String[] items = text.split(";");
        for (String item : items) {
            String[] parts = item.split("\\|", -1);
            if (parts.length < 3) {
                continue;
            }
            try {
                endpoints.add(new Endpoint(
                        emptyToNull(parts[0]),
                        parts[1],
                        Integer.parseInt(parts[2]),
                        parts.length > 3 ? emptyToNull(parts[3]) : null));
            } catch (NumberFormatException ignored) {
            }
        }
        return endpoints;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    public record Endpoint(String nodeId, String host, int port, String state) {
    }
}
