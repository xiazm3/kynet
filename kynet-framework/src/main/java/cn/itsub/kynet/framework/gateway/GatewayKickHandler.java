package cn.itsub.kynet.framework.gateway;

/**
 * Receives server-authoritative kick frames routed to this gateway.
 *
 * <p>Implemented by {@link GatewayRuntime} and wired into the gateway's
 * {@code ServiceNode}; {@code sessionId > 0} targets one connection, otherwise
 * {@code userId > 0} targets every open connection of the user except
 * {@code exceptSessionId}.</p>
 */
@FunctionalInterface
public interface GatewayKickHandler {
    void onKick(long sessionId, long userId, long exceptSessionId, String reason);
}
