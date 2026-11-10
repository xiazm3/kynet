package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.codec.BroadcastPayloadCodec;
import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.codec.GatewayPushWireCodec;
import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.protocol.MessageType;
import cn.itsub.kynet.common.transport.BufferPool;
import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BroadcastPayloadCodecTest {

    @Test
    void supportsGeneratedMessagePayloads() throws Exception {
        byte[] encoded = BroadcastPayloadCodec.encode(StringValue.of("hello"));
        assertEquals("hello", StringValue.parseFrom(encoded).getValue());
    }

    @Test
    void supportsJprotobufPojoPayloads() {
        TestPojo pojo = new TestPojo();
        pojo.setName("alice");

        byte[] encoded = BroadcastPayloadCodec.encode(pojo);
        TestPojo decoded = ProtobufPayloadCodec.decode(encoded, TestPojo.class);

        assertEquals("alice", decoded.getName());
    }

    @Test
    void supportsPrimitivePayloadsViaWrappers() throws Exception {
        assertEquals("hello", StringValue.parseFrom(BroadcastPayloadCodec.encode("hello")).getValue());
        assertEquals(42, Int32Value.parseFrom(BroadcastPayloadCodec.encode(42)).getValue());
    }

    @Test
    void gatewayPushPayloadRemainsStableWhenFrameUsesAnotherBuffer() {
        long sessionId = 100_001L;
        byte[] encodedPayload = BroadcastPayloadCodec.encode("welcome player1");
        int estimatedSize = 8 + 8 + 4 + 4 + 4 + encodedPayload.length;
        UnsafeBuffer payloadBuffer = BufferPool.acquireShared(Math.max(64, estimatedSize));
        try {
            int payloadLength = GatewayPushWireCodec.encode(payloadBuffer, 0, sessionId, 10001L, 5, 6, encodedPayload);

            UnsafeBuffer frameBuffer = BufferPool.acquire();
            int frameLength = FrameCodec.encode(
                    MessageType.GATEWAY_PUSH,
                    6,
                    1,
                    0,
                    0,
                    0L,
                    null,
                    null,
                    null,
                    payloadBuffer,
                    0,
                    payloadLength,
                    frameBuffer,
                    0);

            FrameCodec.DecodedFrame frame = FrameCodec.decodeView(frameBuffer, 0, frameLength);
            GatewayPushWireCodec.GatewayPushView pushView = GatewayPushWireCodec.decodeView(
                    frame.payloadBuffer(),
                    frame.payloadOffset(),
                    frame.payloadLength());

            assertEquals(sessionId, pushView.sessionId());
            assertEquals(10001L, pushView.userId());
            assertEquals(5, pushView.cmd());
            assertEquals(6, pushView.act());
            assertEquals("welcome player1", StringValue.parseFrom(pushView.copyPayload()).getValue());
        } catch (Exception e) {
            throw new AssertionError("unexpected decode failure", e);
        } finally {
            BufferPool.releaseShared(payloadBuffer);
        }
    }

    @ProtobufClass
    public static class TestPojo {
        @Protobuf(fieldType = FieldType.STRING, order = 1)
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
