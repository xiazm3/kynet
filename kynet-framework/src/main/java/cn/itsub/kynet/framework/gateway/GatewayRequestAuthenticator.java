package cn.itsub.kynet.framework.gateway;

@FunctionalInterface
public interface GatewayRequestAuthenticator {
    GatewayAuthenticationResult authenticate(GatewayRequestContext context);
}
