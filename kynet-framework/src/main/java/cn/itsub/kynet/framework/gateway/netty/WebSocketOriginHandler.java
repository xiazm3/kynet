package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.framework.gateway.GatewayServer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

final class WebSocketOriginHandler extends ChannelInboundHandlerAdapter {
    private final GatewayServer gatewayServer;

    WebSocketOriginHandler(GatewayServer gatewayServer) {
        this.gatewayServer = gatewayServer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof FullHttpRequest request)) {
            super.channelRead(ctx, msg);
            return;
        }
        String origin = request.headers().get(HttpHeaderNames.ORIGIN);
        if (gatewayServer.isWebSocketOriginAllowed(origin)) {
            ctx.fireChannelRead(msg);
            return;
        }
        ReferenceCountUtil.release(msg);
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.FORBIDDEN);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        ctx.writeAndFlush(response).addListener(io.netty.channel.ChannelFutureListener.CLOSE);
    }
}
