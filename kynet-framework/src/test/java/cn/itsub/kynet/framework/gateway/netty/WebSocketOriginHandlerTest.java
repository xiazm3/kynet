package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.gateway.GatewayServer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WebSocketOriginHandlerTest {

    @Test
    void secureEndpointRequiresTlsConfiguration() {
        GatewayServer server = new GatewayServer(null, "wss://127.0.0.1:9000/ws");

        assertThrows(IllegalStateException.class, () -> NettyGatewaySslSupport.buildServerContext(
                cn.itsub.kynet.framework.gateway.GatewayEndpoint.parse("wss://127.0.0.1:9000/ws"),
                server));
    }

    @Test
    void rejectsOriginOutsideAllowList() {
        GatewayServer server = new GatewayServer(null, "ws://127.0.0.1:9000/ws");
        server.setAllowedWebSocketOrigins("https://game.example.com");
        EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOriginHandler(server));
        FullHttpRequest request = request("https://attacker.example.com");

        channel.writeInbound(request);

        FullHttpResponse response = channel.readOutbound();
        assertEquals(HttpResponseStatus.FORBIDDEN, response.status());
        response.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void allowsConfiguredOrigin() {
        GatewayServer server = new GatewayServer(null, "ws://127.0.0.1:9000/ws");
        server.setAllowedWebSocketOrigins("https://game.example.com");
        EmbeddedChannel channel = new EmbeddedChannel(new WebSocketOriginHandler(server));
        FullHttpRequest request = request("https://game.example.com");

        channel.writeInbound(request);

        FullHttpRequest forwarded = channel.readInbound();
        assertSame(request, forwarded);
        forwarded.release();
        channel.finishAndReleaseAll();
    }

    private static FullHttpRequest request(String origin) {
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/ws");
        request.headers().set(HttpHeaderNames.ORIGIN, origin);
        return request;
    }
}
