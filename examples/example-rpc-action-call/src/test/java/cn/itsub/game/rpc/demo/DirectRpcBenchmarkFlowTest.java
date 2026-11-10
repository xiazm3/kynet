package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.registry.RegistryServer;
import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.codec.RpcWireCodec;
import cn.itsub.kynet.common.protocol.MessageType;
import cn.itsub.kynet.common.transport.AeronTransport;
import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionExecutionMode;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Ran mvn -pl kynet-examples/example-rpc-action-call -am -Dtest=DirectRpcBenchmarkFlowTest
 *   │ "-Dsurefire.failIfNoSpecifiedTests=false" "-Dkynet.broker.rpcBenchmark.mode=async"
 *   │ "-Dkynet.broker.rpcBenchmark.durationSeconds=10" test
 */
class DirectRpcBenchmarkFlowTest {
    private static final int CMD = 120;
    private static final int INLINE_ACT = 1;
    private static final int ASYNC_ACT = 2;
    private static final int DEFAULT_BROKER_PARTITIONS = 2;
    private static final int DEFAULT_SENDER_THREADS = 8;
    private static final int DEFAULT_MAX_IN_FLIGHT = 16_384;
    private static final int DEFAULT_WARMUP_SECONDS = 2;
    private static final int DEFAULT_DURATION_SECONDS = 3;

    @Test
    @Timeout(120)
    void measuresBrokerRpcFlowFromAToBAndBackToA() throws Exception {
        BenchmarkOptions benchmarkOptions = BenchmarkOptions.fromSystemProperties();
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-rpc-flow-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        String previousBrokerPartitions = System.getProperty("kynet.broker.partitions");
        System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());
        if (previousBrokerPartitions == null || previousBrokerPartitions.isBlank()) {
            System.setProperty("kynet.broker.partitions", String.valueOf(benchmarkOptions.brokerPartitions()));
        }

        int port = AeronTransport.findAvailablePort();
        String brokerDirectory = AeronTransport.brokerAeronDirectory(port);
        MediaDriver mediaDriver = null;
        RegistryServer brokerServer = null;
        ServiceNode serviceA = null;
        ServiceNode serviceB = null;
        try {
            mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .threadingMode(ThreadingMode.DEDICATED)
                    .aeronDirectoryName(brokerDirectory));

            brokerServer = new RegistryServer(port);
            brokerServer.start();

            BenchmarkState state = new BenchmarkState();
            serviceB = new ServiceNode("127.0.0.1", port, "BenchmarkRpcB");
            serviceB.start();
            new ActionBootstrap(serviceB).registerAction(new BenchmarkBAction(state));

            serviceA = new ServiceNode("127.0.0.1", port, "BenchmarkRpcA");
            serviceA.start();

            byte[] payload = payload(benchmarkOptions.payloadBytes());
            InvokeOptions invokeOptions = InvokeOptions.builder()
                    .sessionId(120_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(benchmarkOptions.timeoutMs()))
                    .build();
            int act = actForMode(benchmarkOptions.mode());
            int encodedRequestBytes = estimateEncodedRequestBytes(
                    payload,
                    invokeOptions,
                    ActionKey.of(CMD, act),
                    serviceA.getInstanceId());

            waitForProviderRegistration(serviceA, act, payload);

            if (!benchmarkOptions.warmupDuration().isZero()) {
                BenchmarkResult warmup = runWindow(
                        "broker rpc warmup",
                        benchmarkOptions,
                        serviceA,
                        state,
                        payload,
                        invokeOptions,
                        encodedRequestBytes);
                printResult(warmup);
            }

            BenchmarkResult measured = runWindow(
                    "broker rpc benchmark",
                    benchmarkOptions,
                    serviceA,
                    state,
                    payload,
                    invokeOptions,
                    encodedRequestBytes);
            printResult(measured);

            assertTrue(measured.success() > 0,
                    "benchmark should complete at least one successful RPC request");
        } finally {
            if (serviceA != null) {
                serviceA.shutdown();
            }
            if (serviceB != null) {
                serviceB.shutdown();
            }
            if (brokerServer != null) {
                brokerServer.stop();
            }
            if (mediaDriver != null) {
                mediaDriver.close();
            }
            restoreProperty("kynet.broker.partitions", previousBrokerPartitions);
            restoreProperty("kynet.aeron.baseDir", previousBaseDir);
        }
    }

    private static BenchmarkResult runWindow(String label,
                                             BenchmarkOptions options,
                                             ServiceNode serviceA,
                                             BenchmarkState state,
                                             byte[] payload,
                                             InvokeOptions invokeOptions,
                                             int encodedRequestBytes) throws InterruptedException {
        state.reset();
        AtomicLong sent = new AtomicLong();
        AtomicLong completed = new AtomicLong();
        AtomicLong success = new AtomicLong();
        AtomicLong successInWindow = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong inFlight = new AtomicLong();
        AtomicLong firstSuccessNanos = new AtomicLong();
        AtomicLong lastSuccessNanos = new AtomicLong();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        ConcurrentMap<String, LongAdder> failureCounts = new ConcurrentHashMap<>();
        CountDownLatch sendersDone = new CountDownLatch(options.senderThreads());

        int act = actForMode(options.mode());
        Duration duration = label.contains("warmup") ? options.warmupDuration() : options.duration();
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();
        for (int i = 0; i < options.senderThreads(); i++) {
            Thread thread = new Thread(() -> {
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        long currentInFlight = inFlight.incrementAndGet();
                        if (currentInFlight > options.maxInFlight()) {
                            inFlight.decrementAndGet();
                            Thread.onSpinWait();
                            continue;
                        }

                        sent.incrementAndGet();
                        try {
                            CompletableFuture<ActionResult> future = serviceA.invokeAsync(CMD, act, payload, invokeOptions);
                            future.whenComplete((result, throwable) -> {
                                long completedAtNanos = System.nanoTime();
                                inFlight.decrementAndGet();
                                completed.incrementAndGet();
                                if (throwable != null) {
                                    failed.incrementAndGet();
                                    recordFailure(failureCounts, firstFailure, throwable);
                                    return;
                                }
                                if (result != null && result.isSuccess()) {
                                    success.incrementAndGet();
                                    recordSuccess(firstSuccessNanos, lastSuccessNanos, completedAtNanos);
                                    if (completedAtNanos <= deadlineNanos) {
                                        successInWindow.incrementAndGet();
                                    }
                                    return;
                                }
                                failed.incrementAndGet();
                                recordFailure(failureCounts, firstFailure, result);
                            });
                        } catch (RuntimeException e) {
                            inFlight.decrementAndGet();
                            completed.incrementAndGet();
                            failed.incrementAndGet();
                            recordFailure(failureCounts, firstFailure, e);
                        }
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "broker-rpc-flow-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sendersDone.await();
        long drainDeadlineNanos = System.nanoTime() + options.maxDrainWait().toNanos();
        while (inFlight.get() > 0 && System.nanoTime() < drainDeadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(2);
        }

        return new BenchmarkResult(
                label,
                options.mode(),
                duration,
                options.payloadBytes(),
                encodedRequestBytes,
                options.senderThreads(),
                options.maxInFlight(),
                sent.get(),
                state.handledByB(),
                completed.get(),
                success.get(),
                successInWindow.get(),
                failed.get(),
                inFlight.get(),
                snapshotFailureCounts(failureCounts),
                firstFailure.get(),
                firstSuccessNanos.get(),
                lastSuccessNanos.get());
    }

    private static void waitForProviderRegistration(ServiceNode serviceA, int act, byte[] payload)
            throws InterruptedException {
        InvokeOptions probeOptions = InvokeOptions.builder()
                .sessionId(120_002L)
                .userId(1L)
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        ActionResult lastResult = null;
        RuntimeException lastException = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                lastResult = serviceA.invoke(CMD, act, payload, probeOptions);
                if (lastResult.isSuccess()) {
                    return;
                }
            } catch (RuntimeException e) {
                lastException = e;
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (lastException != null) {
            fail("provider route was not ready before benchmark: " + lastException.getMessage());
        }
        fail("provider route was not ready before benchmark, lastResult=" + describe(lastResult));
    }

    private static void recordSuccess(AtomicLong firstSuccessNanos, AtomicLong lastSuccessNanos, long nowNanos) {
        firstSuccessNanos.compareAndSet(0L, nowNanos);
        lastSuccessNanos.set(nowNanos);
    }

    private static void recordFailure(ConcurrentMap<String, LongAdder> failureCounts,
                                      AtomicReference<String> firstFailure,
                                      ActionResult result) {
        String code = result == null || result.getErrorCode() == null || result.getErrorCode().isBlank()
                ? "UNKNOWN"
                : result.getErrorCode();
        increment(failureCounts, code);
        if (result != null) {
            firstFailure.compareAndSet(null, code + ": " + result.getErrorMessage());
        }
    }

    private static void recordFailure(ConcurrentMap<String, LongAdder> failureCounts,
                                      AtomicReference<String> firstFailure,
                                      Throwable throwable) {
        Throwable cause = rootCause(throwable);
        String code = "EXCEPTION:" + cause.getClass().getSimpleName();
        increment(failureCounts, code);
        firstFailure.compareAndSet(null, code + ": " + cause.getMessage());
    }

    private static void increment(ConcurrentMap<String, LongAdder> failureCounts, String code) {
        failureCounts.computeIfAbsent(code, ignored -> new LongAdder()).increment();
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable instanceof CompletionException && throwable.getCause() != null
                ? throwable.getCause()
                : throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Map<String, Long> snapshotFailureCounts(ConcurrentMap<String, LongAdder> failureCounts) {
        Map<String, Long> snapshot = new TreeMap<>();
        failureCounts.forEach((code, count) -> snapshot.put(code, count.sum()));
        return snapshot;
    }

    private static int estimateEncodedRequestBytes(byte[] payload,
                                                   InvokeOptions options,
                                                   int action,
                                                   int sourceInstanceId) {
        UnsafeBuffer bodyBuffer = payload == null ? null : new UnsafeBuffer(payload);
        UnsafeBuffer rpcPayloadBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Math.max(256,
                160 + (payload == null ? 0 : payload.length))));
        int rpcPayloadLength = RpcWireCodec.encodeRequest(
                rpcPayloadBuffer,
                0,
                options.getSessionId(),
                options.getUserId(),
                options.getGatewayInstanceId(),
                options.getAttachmentData(),
                options.getTimeout().toMillis(),
                false,
                bodyBuffer,
                0,
                payload == null ? 0 : payload.length);
        UnsafeBuffer frameBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(Math.max(512,
                FrameCodec.FIXED_HEADER_LENGTH + rpcPayloadLength + 128)));
        return FrameCodec.encode(
                MessageType.RPC_REQUEST,
                action,
                sourceInstanceId,
                options.getTargetInstanceId(),
                0,
                1L,
                null,
                null,
                null,
                rpcPayloadBuffer,
                0,
                rpcPayloadLength,
                frameBuffer,
                0);
    }

    private static byte[] payload(int payloadBytes) {
        if (payloadBytes <= 0) {
            return new byte[0];
        }
        byte[] payload = new byte[payloadBytes];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0x7F);
        }
        return payload;
    }

    private static int actForMode(String mode) {
        return "async".equalsIgnoreCase(mode) ? ASYNC_ACT : INLINE_ACT;
    }

    private static String normalizeMode(String mode) {
        if ("async".equalsIgnoreCase(mode)) {
            return "async";
        }
        return "inline";
    }

    private static void printResult(BenchmarkResult result) {
        System.out.printf(Locale.ROOT, "%s%n", result.label());
        System.out.printf(Locale.ROOT, "mode: %s%n", result.mode());
        System.out.printf(Locale.ROOT, "duration: %.3f s%n", result.durationSeconds());
        System.out.printf(Locale.ROOT, "business payload: %d bytes%n", result.payloadBytes());
        System.out.printf(Locale.ROOT, "encoded request: %d bytes%n", result.encodedRequestBytes());
        System.out.printf(Locale.ROOT, "senderThreads: %d%n", result.senderThreads());
        System.out.printf(Locale.ROOT, "concurrency: %,d%n", result.maxInFlight());
        System.out.printf(Locale.ROOT, "A sent: %,d (%.2f msg/s)%n", result.sent(), result.sendQps());
        System.out.printf(Locale.ROOT, "B handled: %,d%n", result.handledByB());
        System.out.printf(Locale.ROOT, "A completed after drain: %,d%n", result.completed());
        System.out.printf(Locale.ROOT, "A success after drain: %,d%n", result.success());
        System.out.printf(Locale.ROOT, "A success within window: %,d%n", result.successInWindow());
        System.out.printf(Locale.ROOT, "A RPC success QPS by duration window: %.2f%n", result.successQpsByDuration());
        System.out.printf(Locale.ROOT, "A RPC completed QPS by duration window: %.2f%n", result.completionQpsByDuration());
        System.out.printf(Locale.ROOT, "A RPC success QPS by receive window %.6f s: %.2f%n",
                result.successReceiveSeconds(),
                result.successQpsByReceiveWindow());
        System.out.printf(Locale.ROOT, "failed after drain: %,d%n", result.failed());
        System.out.printf(Locale.ROOT, "pending after drain wait: %,d%n", result.pendingAfterDrain());
        if (!result.failureCounts().isEmpty()) {
            System.out.printf(Locale.ROOT, "failure counts: %s%n", result.failureCounts());
        }
        if (result.firstFailure() != null) {
            System.out.printf(Locale.ROOT, "first failure: %s%n", result.firstFailure());
        }
        System.out.flush();
    }

    private static String describe(ActionResult result) {
        if (result == null) {
            return "null";
        }
        if (result.isSuccess()) {
            return "SUCCESS";
        }
        return result.getErrorCode() + ":" + result.getErrorMessage();
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
    }

    private static int nonNegativeIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value >= 0 ? value : defaultValue;
    }

    private static void restoreProperty(String name, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(name);
            return;
        }
        System.setProperty(name, previousValue);
    }

    private static final class BenchmarkState {
        private final AtomicLong handledByB = new AtomicLong();

        private void reset() {
            handledByB.set(0L);
        }

        private long incrementHandledByB() {
            return handledByB.incrementAndGet();
        }

        private long handledByB() {
            return handledByB.get();
        }
    }

    @ActionController(cmd = CMD)
    public static final class BenchmarkBAction {
        private final BenchmarkState state;

        public BenchmarkBAction(BenchmarkState state) {
            this.state = state;
        }

        @ActionMethod(act = INLINE_ACT, mode = ActionExecutionMode.INLINE)
        public byte[] inlineEcho(byte[] payload, ActionContext context) {
            state.incrementHandledByB();
            return payload;
        }

        @ActionMethod(act = ASYNC_ACT, mode = ActionExecutionMode.ASYNC)
        public byte[] asyncEcho(byte[] payload, ActionContext context) {
            state.incrementHandledByB();
            return payload;
        }
    }

    private record BenchmarkOptions(String mode,
                                    Duration warmupDuration,
                                    Duration duration,
                                    Duration maxDrainWait,
                                    int payloadBytes,
                                    int senderThreads,
                                    int maxInFlight,
                                    int brokerPartitions,
                                    int timeoutMs) {
        private static BenchmarkOptions fromSystemProperties() {
            int warmupSeconds = nonNegativeIntProperty(
                    "kynet.broker.rpcBenchmark.warmupSeconds", DEFAULT_WARMUP_SECONDS);
            int durationSeconds = positiveIntProperty(
                    "kynet.broker.rpcBenchmark.durationSeconds", DEFAULT_DURATION_SECONDS);
            int drainWaitSeconds = nonNegativeIntProperty(
                    "kynet.broker.rpcBenchmark.drainWaitSeconds", 5);
            int payloadBytes = nonNegativeIntProperty(
                    "kynet.broker.rpcBenchmark.payloadBytes", 0);
            int senderThreads = positiveIntProperty(
                    "kynet.broker.rpcBenchmark.senderThreads", DEFAULT_SENDER_THREADS);
            int maxInFlight = positiveIntProperty(
                    "kynet.broker.rpcBenchmark.maxInFlight", DEFAULT_MAX_IN_FLIGHT);
            int brokerPartitions = positiveIntProperty(
                    "kynet.broker.rpcBenchmark.partitions", DEFAULT_BROKER_PARTITIONS);
            int timeoutMs = positiveIntProperty(
                    "kynet.broker.rpcBenchmark.timeoutMs", 3_000);
            String mode = normalizeMode(System.getProperty("kynet.broker.rpcBenchmark.mode", "inline"));
            return new BenchmarkOptions(
                    mode,
                    Duration.ofSeconds(warmupSeconds),
                    Duration.ofSeconds(durationSeconds),
                    Duration.ofSeconds(drainWaitSeconds),
                    payloadBytes,
                    senderThreads,
                    maxInFlight,
                    brokerPartitions,
                    timeoutMs);
        }
    }

    private record BenchmarkResult(String label,
                                   String mode,
                                   Duration duration,
                                   int payloadBytes,
                                   int encodedRequestBytes,
                                   int senderThreads,
                                   int maxInFlight,
                                   long sent,
                                   long handledByB,
                                   long completed,
                                   long success,
                                   long successInWindow,
                                   long failed,
                                   long pendingAfterDrain,
                                   Map<String, Long> failureCounts,
                                   String firstFailure,
                                   long firstSuccessNanos,
                                   long lastSuccessNanos) {
        private double durationSeconds() {
            return duration.toNanos() / 1_000_000_000.0;
        }

        private double sendQps() {
            return durationSeconds() <= 0 ? 0 : sent / durationSeconds();
        }

        private double successQpsByDuration() {
            return durationSeconds() <= 0 ? 0 : successInWindow / durationSeconds();
        }

        private double completionQpsByDuration() {
            return durationSeconds() <= 0 ? 0 : completed / durationSeconds();
        }

        private double successReceiveSeconds() {
            if (firstSuccessNanos == 0L || lastSuccessNanos <= firstSuccessNanos) {
                return 0;
            }
            return (lastSuccessNanos - firstSuccessNanos) / 1_000_000_000.0;
        }

        private double successQpsByReceiveWindow() {
            double seconds = successReceiveSeconds();
            return seconds <= 0 ? 0 : success / seconds;
        }
    }
}
