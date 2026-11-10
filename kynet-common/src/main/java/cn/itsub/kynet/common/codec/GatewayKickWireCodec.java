package cn.itsub.kynet.common.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Wire format of the server-authoritative kick frame.
 *
 * <p>Semantics: {@code sessionId > 0} kicks exactly that gateway connection;
 * otherwise {@code userId > 0} kicks every open connection bound to the user,
 * except the connection whose id equals {@code exceptSessionId} (used for
 * "kick previous logins" / 顶号). {@code reason} is carried for gateway-side logs.</p>
 */
public final class GatewayKickWireCodec {
    private GatewayKickWireCodec() {
    }

    public static int encode(MutableDirectBuffer buffer, int offset,
                             long sessionId, long userId, long exceptSessionId, String reason) {
        int pos = offset;
        buffer.putLong(pos, sessionId);
        pos += 8;
        buffer.putLong(pos, userId);
        pos += 8;
        buffer.putLong(pos, exceptSessionId);
        pos += 8;
        pos = BufferCodecSupport.writeShortString(buffer, pos, reason);
        return pos - offset;
    }

    public static GatewayKickView decodeView(DirectBuffer buffer, int offset, int length) {
        int limit = offset + length;
        long sessionId = offset + 8 <= limit ? buffer.getLong(offset) : 0L;
        int pos = offset + 8;
        long userId = pos + 8 <= limit ? buffer.getLong(pos) : 0L;
        pos += 8;
        long exceptSessionId = pos + 8 <= limit ? buffer.getLong(pos) : 0L;
        pos += 8;
        BufferCodecSupport.ShortStringField reason = BufferCodecSupport.readShortStringField(buffer, pos, limit);
        return new GatewayKickView(buffer, sessionId, userId, exceptSessionId, reason);
    }

    public record GatewayKickView(
            DirectBuffer buffer,
            long sessionId,
            long userId,
            long exceptSessionId,
            BufferCodecSupport.ShortStringField reasonField
    ) {
        public String reason() {
            return reasonField.decode(buffer);
        }
    }
}
