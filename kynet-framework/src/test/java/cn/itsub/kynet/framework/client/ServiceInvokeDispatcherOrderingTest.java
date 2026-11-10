package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.codec.RpcWireCodec;
import cn.itsub.kynet.common.protocol.MessageType;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionInvocation;
import cn.itsub.kynet.framework.action.ActionResult;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServiceInvokeDispatcherOrderingTest {
    private static final int ACTION = 12345;

    @Test
    void sameSessionExecutesActionsSerially() throws Exception {
        ServiceNode client = new ServiceNode("127.0.0.1", 1, "OrderingTest");
        CountDownLatch responses = new CountDownLatch(2);
        List<Long> responseOrder = new CopyOnWriteArrayList<>();
        ServiceInvokeDispatcher dispatcher = new ServiceInvokeDispatcher(client,
                new ServiceInvokeDispatcher.RpcResponseWriter() {
                    @Override
                    public void sendSuccess(long requestId, String traceId, int action,
                                            int targetInstanceId, byte[] body,
                                            boolean attachmentUpdated,
                                            byte[] attachmentData) {
                        responseOrder.add(requestId);
                        responses.countDown();
                    }

                    @Override
                    public void sendError(long requestId, String traceId, int action,
                                          int targetInstanceId, String errorCode, String errorMessage,
                                          boolean attachmentUpdated,
                                          byte[] attachmentData) {
                        responseOrder.add(-requestId);
                        responses.countDown();
                    }
                });
        OrderedHandler handler = new OrderedHandler();
        Method method = OrderedHandler.class.getDeclaredMethod("handle", ActionContext.class);
        dispatcher.registerActionHandler(ACTION,
                new ActionInvocation(handler, method, ACTION, null));

        try {
            dispatcher.handleRpcRequest(requestFrame(1L, 9001L));
            dispatcher.handleRpcRequest(requestFrame(2L, 9001L));

            assertTrue(responses.await(3, TimeUnit.SECONDS));
            assertEquals(List.of(1L, 2L), handler.executionOrder);
            assertEquals(List.of(1L, 2L), responseOrder);
        } finally {
            dispatcher.shutdown();
            client.shutdown();
        }
    }

    private static FrameCodec.DecodedFrame requestFrame(long requestId, long sessionId) {
        UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
        int payloadLength = RpcWireCodec.encodeRequest(
                payload, 0, sessionId, 7L, 0,
                null, 1_000L, false,
                null, 0, 0);
        UnsafeBuffer frame = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int frameLength = FrameCodec.encode(
                MessageType.RPC_REQUEST, ACTION, 88, 0, 0, requestId,
                null, null, null, payload, 0, payloadLength, frame, 0);
        return FrameCodec.decodeView(frame, 0, frameLength);
    }

    public static final class OrderedHandler {
        private final List<Long> executionOrder = new CopyOnWriteArrayList<>();

        public ActionResult handle(ActionContext context) throws InterruptedException {
            if (context.getRequestId() == 1L) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            executionOrder.add(context.getRequestId());
            return ActionResult.success(null);
        }
    }
}
