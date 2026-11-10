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
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServiceInvokeDispatcherIdempotencyTest {
    private static final int ACTION = 23456;

    @Test
    void executesInFlightDuplicateOnceAndReplaysCompletedResponse() throws Exception {
        ServiceNode client = new ServiceNode("127.0.0.1", 1, "IdempotencyTest");
        RecordingWriter writer = new RecordingWriter();
        ServiceInvokeDispatcher dispatcher = new ServiceInvokeDispatcher(client, writer);
        BlockingHandler handler = new BlockingHandler();
        Method method = BlockingHandler.class.getDeclaredMethod("handle", ActionContext.class);
        dispatcher.registerActionHandler(ACTION, new ActionInvocation(handler, method, ACTION, null));

        try {
            dispatcher.handleRpcRequest(requestFrame(41L, true));
            assertTrue(handler.entered.await(3, TimeUnit.SECONDS));
            dispatcher.handleRpcRequest(requestFrame(41L, true));
            handler.release.countDown();

            assertTrue(awaitCount(writer.responses, 2, Duration.ofSeconds(3)));
            assertEquals(1, handler.executions.get());

            dispatcher.handleRpcRequest(requestFrame(41L, true));
            assertTrue(awaitCount(writer.responses, 3, Duration.ofSeconds(3)));
            assertEquals(1, handler.executions.get());
        } finally {
            handler.release.countDown();
            dispatcher.shutdown();
            client.shutdown();
        }
    }

    @Test
    void doesNotDeduplicateOrdinaryRpcRequests() throws Exception {
        ServiceNode client = new ServiceNode("127.0.0.1", 1, "OrdinaryRpcTest");
        RecordingWriter writer = new RecordingWriter();
        ServiceInvokeDispatcher dispatcher = new ServiceInvokeDispatcher(client, writer);
        CountingHandler handler = new CountingHandler();
        Method method = CountingHandler.class.getDeclaredMethod("handle", ActionContext.class);
        dispatcher.registerActionHandler(ACTION, new ActionInvocation(handler, method, ACTION, null));

        try {
            dispatcher.handleRpcRequest(requestFrame(51L, false));
            dispatcher.handleRpcRequest(requestFrame(51L, false));

            assertTrue(awaitCount(writer.responses, 2, Duration.ofSeconds(3)));
            assertEquals(2, handler.executions.get());
        } finally {
            dispatcher.shutdown();
            client.shutdown();
        }
    }

    private static FrameCodec.DecodedFrame requestFrame(long requestId, boolean idempotent) {
        UnsafeBuffer payload = new UnsafeBuffer(ByteBuffer.allocateDirect(128));
        int payloadLength = RpcWireCodec.encodeRequest(
                payload, 0, 9001L, 7L, 0,
                null, 1_000L, false,
                null, 0, 0, idempotent);
        UnsafeBuffer frame = new UnsafeBuffer(ByteBuffer.allocateDirect(256));
        int frameLength = FrameCodec.encode(
                MessageType.RPC_REQUEST, ACTION, 88, 0, 0, requestId,
                null, null, null, payload, 0, payloadLength, frame, 0);
        return FrameCodec.decodeView(frame, 0, frameLength);
    }

    private static boolean awaitCount(AtomicInteger value, int expected, Duration timeout)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (value.get() < expected && System.nanoTime() < deadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        return value.get() >= expected;
    }

    private static final class RecordingWriter implements ServiceInvokeDispatcher.RpcResponseWriter {
        private final AtomicInteger responses = new AtomicInteger();

        @Override
        public void sendSuccess(long requestId, String traceId, int action, int targetInstanceId,
                                byte[] body, boolean attachmentUpdated,
                                byte[] attachmentData) {
            responses.incrementAndGet();
        }

        @Override
        public void sendError(long requestId, String traceId, int action, int targetInstanceId,
                              String errorCode, String errorMessage,
                              boolean attachmentUpdated,
                              byte[] attachmentData) {
            responses.incrementAndGet();
        }
    }

    public static final class BlockingHandler {
        private final AtomicInteger executions = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        public ActionResult handle(ActionContext context) throws InterruptedException {
            executions.incrementAndGet();
            entered.countDown();
            release.await();
            return ActionResult.success("ok");
        }
    }

    public static final class CountingHandler {
        private final AtomicInteger executions = new AtomicInteger();

        public ActionResult handle(ActionContext context) {
            executions.incrementAndGet();
            return ActionResult.success("ok");
        }
    }
}
