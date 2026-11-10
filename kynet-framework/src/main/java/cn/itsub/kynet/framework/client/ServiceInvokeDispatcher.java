package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.codec.RpcWireCodec;
import cn.itsub.kynet.common.transport.BufferPool;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionExecutionMode;
import cn.itsub.kynet.framework.action.ActionInvocation;
import cn.itsub.kynet.framework.action.ActionResult;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

final class ServiceInvokeDispatcher {
    private static final Logger log = LoggerFactory.getLogger(ServiceInvokeDispatcher.class);
    private static final int RPC_MAX_CONCURRENT_ACTIONS = Math.max(1,
            Integer.getInteger("kynet.rpc.maxConcurrentActions", 16_384));
    private static final int RPC_SERIAL_STRIPES = Math.max(1,
            Integer.getInteger("kynet.rpc.serialStripes", 1_024));
    private static final int RPC_DEDUP_MAX_ENTRIES = Math.max(1,
            Integer.getInteger("kynet.rpc.dedup.maxEntries", 100_000));
    private static final long RPC_DEDUP_TTL_MILLIS = Math.max(1L,
            Long.getLong("kynet.rpc.dedup.ttlMillis", 60_000L));
    private static final long RPC_DEDUP_IN_FLIGHT_TTL_MILLIS = Math.max(1L,
            Long.getLong("kynet.rpc.dedup.inFlightTtlMillis", 30_000L));

    private final ServiceNode serviceNode;
    private final RpcResponseWriter responseWriter;
    private final ExecutorService rpcExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final SerialStripe[] serialStripes = new SerialStripe[RPC_SERIAL_STRIPES];
    private final Semaphore rpcActionPermits = new Semaphore(RPC_MAX_CONCURRENT_ACTIONS);
    private final AtomicLong rejectedOneWayActions = new AtomicLong();
    private final RpcRequestDeduplicator requestDeduplicator = new RpcRequestDeduplicator(
            RPC_DEDUP_MAX_ENTRIES,
            Duration.ofMillis(RPC_DEDUP_TTL_MILLIS),
            Duration.ofMillis(RPC_DEDUP_IN_FLIGHT_TTL_MILLIS));
    private final Object handlerMapLock = new Object();
    private volatile Int2ObjectHashMap<ActionInvocation> actionHandlers = new Int2ObjectHashMap<>();
    private volatile Int2ObjectHashMap<ServiceNode.FastOneWayHandler> fastOneWayHandlers = new Int2ObjectHashMap<>();
    private volatile FastHandlerCache fastHandlerCache;
    private volatile ServiceNode.FastOneWayHandler defaultFastOneWayHandler;

    ServiceInvokeDispatcher(ServiceNode serviceNode, RpcResponseWriter responseWriter) {
        this.serviceNode = serviceNode;
        this.responseWriter = responseWriter;
        for (int i = 0; i < serialStripes.length; i++) {
            serialStripes[i] = new SerialStripe();
        }
    }

    void shutdown() {
        rpcExecutor.shutdownNow();
        requestDeduplicator.clear();
    }

    void registerActionHandler(int action, ActionInvocation invocation) {
        synchronized (handlerMapLock) {
            Int2ObjectHashMap<ActionInvocation> next = new Int2ObjectHashMap<>(actionHandlers);
            next.put(action, invocation);
            actionHandlers = next;
        }
    }

    void setFastOneWayHandler(ServiceNode.FastOneWayHandler handler) {
        defaultFastOneWayHandler = handler;
    }

    void registerFastOneWayHandler(int action, ServiceNode.FastOneWayHandler handler) {
        synchronized (handlerMapLock) {
            Int2ObjectHashMap<ServiceNode.FastOneWayHandler> next = new Int2ObjectHashMap<>(fastOneWayHandlers);
            if (handler == null) {
                next.remove(action);
            } else {
                next.put(action, handler);
            }
            fastOneWayHandlers = next;
            fastHandlerCache = next.size() == 1 ? singleFastHandler(next) : null;
        }
    }

    private static FastHandlerCache singleFastHandler(
            Int2ObjectHashMap<ServiceNode.FastOneWayHandler> handlers) {
        var iterator = handlers.entrySet().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        var entry = iterator.next();
        return new FastHandlerCache(entry.getKey(), entry.getValue());
    }

    private record FastHandlerCache(int action, ServiceNode.FastOneWayHandler handler) {
    }

    void handleRpcRequest(FrameCodec.DecodedFrame frame) {
        handleRpcRequest(frame, 0L);
    }

    void handleRpcRequest(FrameCodec.DecodedFrame frame, long sourceIncarnation) {
        RpcWireCodec.RpcRequestView requestView = RpcWireCodec.decodeRequestView(
                frame.payloadBuffer(),
                frame.payloadOffset(),
                frame.payloadLength());
        RpcFrameMeta receivedMeta = new RpcFrameMeta(
                frame.requestId(),
                frame.traceId(),
                frame.action(),
                frame.sourceInstanceId(),
                sourceIncarnation,
                null);

        if (requestView.oneWay() && tryHandleFastOneWay(receivedMeta, requestView)) {
            return;
        }
        if (requestView.oneWay() && tryExecuteInlineOneWay(receivedMeta, requestView)) {
            return;
        }
        RpcFrameMeta frameMeta = prepareIdempotentRequest(receivedMeta, requestView);
        if (frameMeta == null) {
            return;
        }
        if (!requestView.oneWay() && tryExecuteInlineRpcRequest(frameMeta, requestView)) {
            return;
        }

        if (!rpcActionPermits.tryAcquire()) {
            if (!requestView.oneWay()) {
                discardDeduplication(frameMeta);
                sendRpcExecutorBusy(frameMeta);
            } else {
                logRejectedOneWayAction(frameMeta.action(), "max concurrent actions is reached");
            }
            return;
        }

        RetainedRpcRequest retainedRequest = null;
        try {
            ActionInvocation invocation = actionHandlers.get(frameMeta.action());
            retainedRequest = retainRequest(requestView,
                    invocation != null && invocation.expectsByteArrayPayload());
            RetainedRpcRequest requestForTask = retainedRequest;
            if (requestView.oneWay()) {
                executeRpcTask(routingKey(requestView.sessionId(), requestView.userId()), () -> {
                    try {
                        dispatchOneWayRpcRequest(frameMeta, requestForTask);
                    } finally {
                        releaseRpcActionPermit();
                    }
                });
                return;
            }
            executeRpcTask(routingKey(requestView.sessionId(), requestView.userId()),
                    () -> dispatchRpcRequest(frameMeta, requestForTask));
        } catch (RejectedExecutionException e) {
            releaseRetainedRequest(retainedRequest);
            releaseRpcActionPermit();
            if (!requestView.oneWay()) {
                discardDeduplication(frameMeta);
                sendRpcExecutorBusy(frameMeta);
            } else {
                logRejectedOneWayAction(frameMeta.action(), "RPC executor is busy");
            }
        } catch (RuntimeException e) {
            releaseRetainedRequest(retainedRequest);
            releaseRpcActionPermit();
            if (!requestView.oneWay()) {
                sendRpcError(frameMeta, "BAD_RPC_REQUEST", e.getMessage(),
                        false, null);
            } else {
                log.warn("Drop one-way action {} because request retain failed: {}",
                        frameMeta.action(), e.getMessage());
            }
        }
    }

    /**
     * Allocation-free fast path for one-way frames: parses the action, user and body
     * directly from the wire buffer instead of building DecodedFrame/RpcRequestView/RpcFrameMeta.
     *
     * @return true when a fast one-way handler consumed the frame
     */
    boolean tryHandleFastOneWayFrame(DirectBuffer buffer, int frameOffset, int frameLength) {
        int action = FrameCodec.readAction(buffer, frameOffset);
        FastHandlerCache cache = fastHandlerCache;
        ServiceNode.FastOneWayHandler handler;
        if (cache != null && cache.action() == action) {
            handler = cache.handler();
        } else {
            handler = fastOneWayHandlers.get(action);
            if (handler == null) {
                handler = defaultFastOneWayHandler;
                if (handler == null) {
                    return false;
                }
            }
        }
        int limit = frameOffset + frameLength;
        long requestId = buffer.getLong(frameOffset + FrameCodec.FIXED_HEADER_LENGTH);
        // Fast senders emit empty topic/traceId/payloadType strings, which makes every
        // following field offset constant. This covers the hot path without the general
        // variable-length walk below.
        int fixed = frameOffset + FrameCodec.FIXED_HEADER_LENGTH + Long.BYTES;
        if (fixed + Short.BYTES * 3 + 41 <= limit
                && buffer.getShort(fixed) == 0
                && buffer.getShort(fixed + Short.BYTES) == 0
                && buffer.getShort(fixed + Short.BYTES * 2) == 0) {
            int payloadOffset = fixed + Short.BYTES * 3;
            int attachmentLength = buffer.getInt(payloadOffset + 20);
            if (attachmentLength == 0
                    && buffer.getByte(payloadOffset + 32) != 0) {
                int bodyLength = Math.max(0, buffer.getInt(payloadOffset + 33));
                int bodyOffset = payloadOffset + 37;
                bodyLength = (int) Math.min(bodyLength, (long) limit - bodyOffset);
                try {
                    handler.onOneWay(action, requestId, buffer.getLong(payloadOffset + 8),
                            buffer.getInt(payloadOffset + 16), buffer, bodyOffset,
                            Math.max(0, bodyLength));
                } catch (Exception e) {
                    log.error("Fast one-way handler failed for action {}", action, e);
                }
                return true;
            }
        }
        int pos = FrameCodec.payloadOffset(buffer, frameOffset, frameLength);
        if (pos + Long.BYTES > limit) {
            return false;
        }
        pos += Long.BYTES;
        long userId = pos + Long.BYTES <= limit ? buffer.getLong(pos) : 0L;
        pos += Long.BYTES;
        int gatewayInstanceId = pos + Integer.BYTES <= limit ? buffer.getInt(pos) : 0;
        pos += Integer.BYTES;
        int attachmentLength = pos + Integer.BYTES <= limit ? Math.max(0, buffer.getInt(pos)) : 0;
        pos = (int) Math.min(limit, (long) pos + Integer.BYTES + attachmentLength);
        pos = (int) Math.min(limit, (long) pos + Long.BYTES);
        boolean oneWay = pos < limit && buffer.getByte(pos) != 0;
        if (!oneWay) {
            return false;
        }
        pos++;
        int bodyLength = pos + Integer.BYTES <= limit ? Math.max(0, buffer.getInt(pos)) : 0;
        pos += Integer.BYTES;
        int bodyOffset = Math.min(limit, pos);
        bodyLength = (int) Math.min(bodyLength, (long) limit - bodyOffset);
        try {
            handler.onOneWay(action, requestId, userId, gatewayInstanceId,
                    buffer, bodyOffset, Math.max(0, bodyLength));
        } catch (Exception e) {
            log.error("Fast one-way handler failed for action {}", action, e);
        }
        return true;
    }

    private boolean tryHandleFastOneWay(RpcFrameMeta frameMeta, RpcWireCodec.RpcRequestView requestView) {
        ServiceNode.FastOneWayHandler handler = fastOneWayHandlers.get(frameMeta.action());
        if (handler == null) {
            handler = defaultFastOneWayHandler;
        }
        if (handler == null) {
            return false;
        }
        try {
            handler.onOneWay(
                    frameMeta.action(),
                    frameMeta.requestId(),
                    requestView.userId(),
                    requestView.gatewayInstanceId(),
                    requestView.bodyBuffer(),
                    requestView.bodyOffset(),
                    requestView.bodyLength());
        } catch (Exception e) {
            log.error("Fast one-way handler failed for action {}", frameMeta.action(), e);
        }
        return true;
    }

    private boolean tryExecuteInlineOneWay(RpcFrameMeta frameMeta, RpcWireCodec.RpcRequestView requestView) {
        ActionInvocation invocation = actionHandlers.get(frameMeta.action());
        if (invocation == null || invocation.executionMode() != ActionExecutionMode.INLINE) {
            return false;
        }
        ActionContext context = new ActionContext(serviceNode, frameMeta.requestId(), frameMeta.traceId(),
                requestView.sessionId(), requestView.userId(), requestView.gatewayInstanceId(),
                requestView.attachmentBuffer(), requestView.attachmentOffset(), requestView.attachmentLength());
        try {
            invocation.invoke(requestView.bodyBuffer(), requestView.bodyOffset(), requestView.bodyLength(), context);
        } catch (Exception e) {
            log.error("Invoke inline one-way action {} failed", frameMeta.action(), e);
        }
        return true;
    }

    private boolean tryExecuteInlineRpcRequest(RpcFrameMeta frameMeta, RpcWireCodec.RpcRequestView requestView) {
        ActionInvocation invocation = actionHandlers.get(frameMeta.action());
        if (invocation == null || invocation.executionMode() != ActionExecutionMode.INLINE) {
            return false;
        }
        ActionContext context = new ActionContext(serviceNode, frameMeta.requestId(), frameMeta.traceId(),
                requestView.sessionId(), requestView.userId(), requestView.gatewayInstanceId(),
                requestView.attachmentBuffer(), requestView.attachmentOffset(), requestView.attachmentLength());
        try {
            Object value = invocation.invoke(requestView.bodyBuffer(), requestView.bodyOffset(), requestView.bodyLength(), context);
            if (value instanceof CompletionStage<?>) {
                sendRpcError(frameMeta, "INLINE_ASYNC_RESULT",
                        "Inline RPC action " + frameMeta.action() + " returned CompletionStage; use ASYNC mode for asynchronous actions.",
                        context.isAttachmentUpdated(), context.getAttachmentData());
                return true;
            }
            sendRpcInvocationResultSafely(frameMeta, context, value);
        } catch (Exception e) {
            sendRpcInvocationErrorSafely(frameMeta, context, e);
        }
        return true;
    }

    private void dispatchOneWayRpcRequest(RpcFrameMeta frameMeta, RetainedRpcRequest request) {
        try {
            executeOneWayRpcRequest(frameMeta, request);
        } finally {
            request.release();
        }
    }

    private void executeOneWayRpcRequest(RpcFrameMeta frameMeta, RetainedRpcRequest request) {
        ActionInvocation invocation = actionHandlers.get(frameMeta.action());
        if (invocation == null) {
            return;
        }
        ActionContext context = new ActionContext(serviceNode, frameMeta.requestId(), frameMeta.traceId(),
                request.sessionId(), request.userId(), request.gatewayInstanceId(),
                request.attachmentData());
        try {
            Object value = invokeRetainedRequest(invocation, request, context);
            if (routingKey(request.sessionId(), request.userId()) != 0L
                    && value instanceof CompletionStage<?> stage) {
                stage.toCompletableFuture().join();
            }
        } catch (Exception e) {
            log.error("Invoke one-way action {} failed", frameMeta.action(), e);
        }
    }

    private void dispatchRpcRequest(RpcFrameMeta frameMeta, RetainedRpcRequest request) {
        try {
            executeRpcRequest(frameMeta, request);
        } catch (Exception e) {
            log.warn("RPC action {} execution failed after dispatch: {}", frameMeta.action(), e.getMessage());
        } finally {
            request.release();
        }
    }

    private void executeRpcRequest(RpcFrameMeta frameMeta, RetainedRpcRequest request) {
        ActionInvocation invocation = actionHandlers.get(frameMeta.action());
        if (invocation == null) {
            try {
                sendRpcError(frameMeta, "ACTION_NOT_FOUND",
                        "No local handler for action " + frameMeta.action(), false, null);
            } finally {
                releaseRpcActionPermit();
            }
            return;
        }
        ActionContext context = null;
        boolean completionDeferred = false;
        try {
            context = new ActionContext(serviceNode, frameMeta.requestId(), frameMeta.traceId(),
                    request.sessionId(), request.userId(), request.gatewayInstanceId(),
                    request.attachmentData());
            Object value = invokeRetainedRequest(invocation, request, context);
            if (value instanceof CompletionStage<?> stage) {
                if (routingKey(request.sessionId(), request.userId()) != 0L) {
                    Object asyncValue = stage.toCompletableFuture().join();
                    sendRpcInvocationResultSafely(frameMeta, context, asyncValue);
                    return;
                }
                completeRpcRequestWhenStageCompletes(frameMeta, context, stage);
                completionDeferred = true;
                return;
            }
            sendRpcInvocationResultSafely(frameMeta, context, value);
        } catch (Exception e) {
            if (context != null) {
                sendRpcInvocationErrorSafely(frameMeta, context, e);
            } else {
                sendRpcError(frameMeta, "ACTION_EXECUTION_ERROR", e.getMessage(),
                        false, null);
            }
        } finally {
            if (!completionDeferred) {
                releaseRpcActionPermit();
            }
        }
    }

    private Object invokeRetainedRequest(ActionInvocation invocation, RetainedRpcRequest request,
                                         ActionContext context) throws Exception {
        byte[] bodyData = request.bodyData();
        if (bodyData != null) {
            return invocation.invoke(bodyData, context);
        }
        return invocation.invoke(request.bodyBuffer(), 0, request.bodyLength(), context);
    }

    private void completeRpcRequestWhenStageCompletes(RpcFrameMeta frameMeta, ActionContext context,
                                                      CompletionStage<?> stage) {
        stage.whenComplete((asyncValue, throwable) -> {
            try {
                rpcExecutor.execute(() -> {
                    try {
                        if (throwable != null) {
                            sendRpcInvocationErrorSafely(frameMeta, context, throwable);
                            return;
                        }
                        sendRpcInvocationResultSafely(frameMeta, context, asyncValue);
                    } finally {
                        releaseRpcActionPermit();
                    }
                });
            } catch (RejectedExecutionException e) {
                releaseRpcActionPermit();
                log.warn("Drop async RPC response for action {} because executor is shutting down", frameMeta.action());
            }
        });
    }

    private void executeRpcTask(long routingKey, Runnable task) {
        if (routingKey == 0L) {
            rpcExecutor.execute(task);
            return;
        }
        int stripe = Math.floorMod((int) (routingKey ^ (routingKey >>> 32)), serialStripes.length);
        serialStripes[stripe].execute(task);
    }

    private static long routingKey(long sessionId, long userId) {
        return sessionId != 0L ? sessionId : userId;
    }

    private void logRejectedOneWayAction(int action, String reason) {
        long rejected = rejectedOneWayActions.incrementAndGet();
        if ((rejected & (rejected - 1L)) == 0L) {
            log.warn("Drop one-way action {} because {}, rejectedTotal={}", action, reason, rejected);
        }
    }

    private RpcFrameMeta prepareIdempotentRequest(RpcFrameMeta frameMeta,
                                                   RpcWireCodec.RpcRequestView requestView) {
        if (requestView.oneWay() || !requestView.idempotent()) {
            return frameMeta;
        }
        RpcRequestDeduplicator.AcquireResult acquisition = requestDeduplicator.acquire(
                frameMeta.sourceInstanceId(), frameMeta.sourceIncarnation(), frameMeta.requestId(), frameMeta.action());
        return switch (acquisition.status()) {
            case OWNER -> frameMeta.withDeduplicationEntry(acquisition.entry());
            case DUPLICATE -> {
                replayDuplicateResponse(frameMeta, acquisition.entry());
                yield null;
            }
            case CONFLICT -> {
                sendRpcErrorDirect(frameMeta, "IDEMPOTENCY_KEY_REUSED",
                        "RPC requestId was reused for another action", false, null);
                yield null;
            }
            case CAPACITY_EXCEEDED -> {
                sendRpcErrorDirect(frameMeta, "IDEMPOTENCY_CACHE_FULL",
                        "RPC idempotency cache capacity is exhausted", false, null);
                yield null;
            }
        };
    }

    private void replayDuplicateResponse(RpcFrameMeta frameMeta, RpcRequestDeduplicator.Entry entry) {
        entry.response().whenComplete((response, failure) -> {
            if (failure != null) {
                log.warn("Cannot replay idempotent RPC response for action {}: {}",
                        frameMeta.action(), failure.getMessage());
                return;
            }
            try {
                writeCachedResponse(frameMeta, response);
            } catch (RuntimeException e) {
                log.warn("Failed to replay idempotent RPC response for action {}: {}",
                        frameMeta.action(), e.getMessage());
            }
        });
    }

    private void sendRpcInvocationResult(RpcFrameMeta frameMeta, ActionContext context, Object value) {
        if (value instanceof ActionResult ar) {
            boolean attachmentUpdated = ar.isAttachmentUpdated() || context.isAttachmentUpdated();
            byte[] attachmentData = ar.isAttachmentUpdated() ? ar.getAttachmentData() : context.getAttachmentData();
            if (ar.isSuccess()) {
                sendRpcSuccess(frameMeta, ProtobufPayloadCodec.encode(ar.getData()),
                        attachmentUpdated, attachmentData);
            } else {
                sendRpcError(frameMeta, ar.getErrorCode(), ar.getErrorMessage(),
                        attachmentUpdated, attachmentData);
            }
            return;
        }
        sendRpcSuccess(frameMeta, ProtobufPayloadCodec.encode(value),
                context.isAttachmentUpdated(), context.getAttachmentData());
    }

    private void sendRpcInvocationResultSafely(RpcFrameMeta frameMeta, ActionContext context, Object value) {
        try {
            sendRpcInvocationResult(frameMeta, context, value);
        } catch (RuntimeException e) {
            log.warn("Failed to send RPC response for action {}: {}", frameMeta.action(), e.getMessage());
        }
    }

    private void sendRpcInvocationError(RpcFrameMeta frameMeta, ActionContext context, Throwable throwable) {
        Throwable cause = unwrapCompletionException(throwable);
        log.error("Invoke action {} failed", frameMeta.action(), cause);
        sendRpcError(frameMeta, "ACTION_EXECUTION_ERROR", cause.getMessage(),
                context.isAttachmentUpdated(), context.getAttachmentData());
    }

    private void sendRpcInvocationErrorSafely(RpcFrameMeta frameMeta, ActionContext context, Throwable throwable) {
        try {
            sendRpcInvocationError(frameMeta, context, throwable);
        } catch (RuntimeException e) {
            log.warn("Failed to send RPC error response for action {}: {}", frameMeta.action(), e.getMessage());
        }
    }

    private void sendRpcExecutorBusy(RpcFrameMeta frameMeta) {
        try {
            sendRpcErrorDirect(frameMeta, "RPC_EXECUTOR_BUSY",
                    "RPC max concurrent actions reached for action " + frameMeta.action(),
                    false, null);
        } catch (RuntimeException sendFailure) {
            log.warn("Failed to send RPC executor busy response for action {}: {}",
                    frameMeta.action(), sendFailure.getMessage());
        }
    }

    private void releaseRpcActionPermit() {
        rpcActionPermits.release();
    }

    private void releaseRetainedRequest(RetainedRpcRequest request) {
        if (request != null) {
            request.release();
        }
    }

    private Throwable unwrapCompletionException(Throwable throwable) {
        if (throwable instanceof CompletionException && throwable.getCause() != null) {
            return throwable.getCause();
        }
        return throwable;
    }

    private RetainedRpcRequest retainRequest(RpcWireCodec.RpcRequestView request, boolean retainBodyAsByteArray) {
        UnsafeBuffer bodyBuffer = null;
        byte[] bodyData = null;
        int bodyLength = request.bodyLength();
        if (bodyLength > 0) {
            if (retainBodyAsByteArray) {
                bodyData = new byte[bodyLength];
                request.bodyBuffer().getBytes(request.bodyOffset(), bodyData);
            } else {
                bodyBuffer = BufferPool.acquireShared(bodyLength);
                bodyBuffer.putBytes(0, request.bodyBuffer(), request.bodyOffset(), bodyLength);
            }
        }
        return new RetainedRpcRequest(
                request.sessionId(),
                request.userId(),
                request.gatewayInstanceId(),
                copyOptionalBytes(request.attachmentBuffer(), request.attachmentOffset(), request.attachmentLength()),
                bodyData,
                bodyBuffer,
                bodyLength);
    }

    private byte[] copyOptionalBytes(DirectBuffer buffer, int offset, int length) {
        if (buffer == null || length <= 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return bytes;
    }

    interface RpcResponseWriter {
        default void sendSuccess(long requestId, String traceId, int action, int targetInstanceId,
                                 byte[] body, boolean attachmentUpdated,
                                 byte[] attachmentData, long incarnation) {
            sendSuccess(requestId, traceId, action, targetInstanceId, body, attachmentUpdated, attachmentData);
        }
        default void sendError(long requestId, String traceId, int action, int targetInstanceId,
                               String errorCode, String errorMessage, boolean attachmentUpdated,
                               byte[] attachmentData, long incarnation) {
            sendError(requestId, traceId, action, targetInstanceId, errorCode, errorMessage, attachmentUpdated, attachmentData);
        }
        void sendSuccess(long requestId, String traceId, int action, int targetInstanceId,
                         byte[] body, boolean attachmentUpdated,
                         byte[] attachmentData);

        void sendError(long requestId, String traceId, int action, int targetInstanceId,
                       String errorCode, String errorMessage,
                       boolean attachmentUpdated,
                       byte[] attachmentData);
    }

    private final class SerialStripe {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
        private boolean draining;

        synchronized void execute(Runnable task) {
            tasks.addLast(task);
            if (draining) {
                return;
            }
            draining = true;
            try {
                rpcExecutor.execute(this::drain);
            } catch (RuntimeException e) {
                draining = false;
                tasks.removeLastOccurrence(task);
                throw e;
            }
        }

        private void drain() {
            while (true) {
                Runnable task;
                synchronized (this) {
                    task = tasks.pollFirst();
                    if (task == null) {
                        draining = false;
                        return;
                    }
                }
                try {
                    task.run();
                } catch (RuntimeException e) {
                    log.warn("Serial RPC task failed: {}", e.getMessage());
                }
            }
        }
    }

    private void sendRpcSuccess(RpcFrameMeta frameMeta, byte[] body,
                                boolean attachmentUpdated, byte[] attachmentData) {
        if (frameMeta.deduplicationEntry() == null) {
            responseWriter.sendSuccess(frameMeta.requestId(), frameMeta.traceId(), frameMeta.action(),
                    frameMeta.sourceInstanceId(), body, attachmentUpdated,
                    attachmentData, frameMeta.sourceIncarnation());
            return;
        }
        RpcRequestDeduplicator.CachedResponse response = new RpcRequestDeduplicator.CachedResponse(
                true, null, null, attachmentUpdated, attachmentData, body);
        writeCachedResponse(frameMeta, completeDeduplication(frameMeta, response));
    }

    private void sendRpcError(RpcFrameMeta frameMeta, String errorCode, String errorMessage,
                              boolean attachmentUpdated, byte[] attachmentData) {
        if (frameMeta.deduplicationEntry() == null) {
            sendRpcErrorDirect(frameMeta, errorCode, errorMessage, attachmentUpdated, attachmentData);
            return;
        }
        RpcRequestDeduplicator.CachedResponse response = new RpcRequestDeduplicator.CachedResponse(
                false, errorCode, errorMessage, attachmentUpdated, attachmentData, null);
        writeCachedResponse(frameMeta, completeDeduplication(frameMeta, response));
    }

    private void sendRpcErrorDirect(RpcFrameMeta frameMeta, String errorCode, String errorMessage,
                                    boolean attachmentUpdated, byte[] attachmentData) {
        responseWriter.sendError(frameMeta.requestId(), frameMeta.traceId(), frameMeta.action(),
                frameMeta.sourceInstanceId(), errorCode, errorMessage,
                attachmentUpdated, attachmentData, frameMeta.sourceIncarnation());
    }

    private void writeCachedResponse(RpcFrameMeta frameMeta, RpcRequestDeduplicator.CachedResponse response) {
        if (response.success()) {
            responseWriter.sendSuccess(frameMeta.requestId(), frameMeta.traceId(), frameMeta.action(),
                    frameMeta.sourceInstanceId(), response.body(),
                    response.attachmentUpdated(), response.attachmentData(), frameMeta.sourceIncarnation());
            return;
        }
        sendRpcErrorDirect(frameMeta, response.errorCode(), response.errorMessage(),
                response.attachmentUpdated(), response.attachmentData());
    }

    private RpcRequestDeduplicator.CachedResponse completeDeduplication(
            RpcFrameMeta frameMeta, RpcRequestDeduplicator.CachedResponse response) {
        RpcRequestDeduplicator.Entry entry = frameMeta.deduplicationEntry();
        if (entry == null || entry.complete(response)) {
            return response;
        }
        return entry.response().join();
    }

    private void discardDeduplication(RpcFrameMeta frameMeta) {
        requestDeduplicator.discard(frameMeta.deduplicationEntry());
    }

    private record RpcFrameMeta(long requestId, String traceId, int action, int sourceInstanceId,
                                long sourceIncarnation,
                                RpcRequestDeduplicator.Entry deduplicationEntry) {
        private RpcFrameMeta withDeduplicationEntry(RpcRequestDeduplicator.Entry entry) {
            return new RpcFrameMeta(requestId, traceId, action, sourceInstanceId, sourceIncarnation, entry);
        }
    }

    private record RetainedRpcRequest(long sessionId, long userId, int gatewayInstanceId,
                                      byte[] attachmentData,
                                      byte[] bodyData, UnsafeBuffer bodyBuffer, int bodyLength) {
        private void release() {
            BufferPool.releaseShared(bodyBuffer);
        }
    }
}
