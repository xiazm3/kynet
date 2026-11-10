package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.common.codec.RpcWireCodec.RpcResponseView;
import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionExecutionMode;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 连接一个已经启动的 broker，测量两个服务之间的极限 Action QPS：
 * 服务 A 注册零业务逻辑的 INLINE Action，服务 B 以最高并发持续调用。包含两个用例：
 * <ul>
 *   <li>{@link #measuresMaxActionQpsAgainstRunningBroker}：RPC 往返 QPS（FastRpcSender + FastRpcCallback）</li>
 *   <li>{@link #measuresMaxOneWayQpsAgainstRunningBroker}：单向 QPS（FastOneWaySender + FastOneWayHandler）</li>
 * </ul>
 *
 * <p>前置条件：broker 已启动（默认 {@code 127.0.0.1:8888}）。如果连接不上 broker，
 * 测试会自动 skip，不会让 {@code mvn test} 失败。</p>
 *
 * <p>运行方式（在 kynet 根目录）：</p>
 * <pre>
 * mvn -pl kynet-examples/example-rpc-action-call -am test -Dtest=DirectActionMaxQpsTest
 * </pre>
 *
 * <p>可调系统属性：</p>
 * <ul>
 *   <li>{@code kynet.broker.external.host} / {@code kynet.broker.external.port}：已启动的 broker 地址，默认 127.0.0.1:8888</li>
 *   <li>{@code kynet.broker.maxQps.senderThreads}：压测线程数，默认 min(CPU 核数, 8)，至少 2</li>
 *   <li>{@code kynet.broker.maxQps.maxInFlight}：全局最大在途请求数，
 *       默认按 {@code kynet.direct.peerQueueBytes} / 512 估算（4MB 队列对应 8192）；
 *       direct 传输的每对端队列默认只有 4MB，在途窗口调得远大于队列容量会立刻反压，
 *       表现为大量 {@code RESULT_UNKNOWN}。想压得更深时请同时调大
 *       {@code kynet.direct.peerQueueBytes} / {@code kynet.direct.queueBytes}</li>
 *   <li>{@code kynet.broker.maxQps.warmupSeconds}：预热秒数，默认 3</li>
 *   <li>{@code kynet.broker.maxQps.durationSeconds}：正式测量秒数，默认 10</li>
 *   <li>{@code kynet.broker.maxQps.timeoutMs}：单次 RPC 超时，默认 3000</li>
 * </ul>
 */
class DirectActionMaxQpsTest {
    private static final int CMD = 202;
    private static final int ACT = 1;
    private static final int ONE_WAY_CMD = 203;
    private static final int ONE_WAY_ACT = 1;

    private static final String DEFAULT_BROKER_HOST = "127.0.0.1";
    private static final int DEFAULT_BROKER_PORT = 8888;

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void measuresMaxActionQpsAgainstRunningBroker() throws Exception {
        String host = System.getProperty("kynet.broker.external.host", DEFAULT_BROKER_HOST);
        int port = Integer.getInteger("kynet.broker.external.port", DEFAULT_BROKER_PORT);
        int senderThreads = positiveIntProperty("kynet.broker.maxQps.senderThreads",
                Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors())));
        int maxInFlight = positiveIntProperty("kynet.broker.maxQps.maxInFlight",
                defaultMaxInFlight());
        int warmupSeconds = nonNegativeIntProperty("kynet.broker.maxQps.warmupSeconds", 3);
        int durationSeconds = positiveIntProperty("kynet.broker.maxQps.durationSeconds", 10);
        int timeoutMs = positiveIntProperty("kynet.broker.maxQps.timeoutMs", 3_000);

        ServiceNode provider = new ServiceNode(host, port, "MaxQpsServiceA");
        ServiceNode caller = new ServiceNode(host, port, "MaxQpsServiceB");
        try {
            try {
                provider.start();
            } catch (RuntimeException e) {
                Assumptions.abort("外部 broker 不可达 " + host + ":" + port
                        + "，请先启动 broker。原因: " + rootCause(e));
            }

            MaxQpsAction action = new MaxQpsAction();
            new ActionBootstrap(provider).registerAction(action);
            caller.start();

            InvokeOptions options = InvokeOptions.builder()
                    .sessionId(202_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(timeoutMs))
                    .build();

            waitForActionReady(caller, options);

            if (warmupSeconds > 0) {
                ThroughputResult warmup = runWindow(caller, options, senderThreads, maxInFlight,
                        Duration.ofSeconds(warmupSeconds), action);
                printResult("max qps warmup", warmup);
            }

            ThroughputResult measured = runWindow(caller, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(durationSeconds), action);
            printResult("max qps", measured);
            System.out.printf(
                    "max qps transport: ipcSent=%d, udpSent=%d, dropped=%d, queuedBytes=%d%n",
                    caller.directIpcSentCount(), caller.directUdpSentCount(),
                    caller.directDroppedCount(), caller.directQueuedBytes());

            assertTrue(measured.success() > 0, "测量窗口内至少应完成一次成功的 Action 调用");
        } finally {
            caller.shutdown();
            provider.shutdown();
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void measuresMaxOneWayQpsAgainstRunningBroker() throws Exception {
        String host = System.getProperty("kynet.broker.external.host", DEFAULT_BROKER_HOST);
        int port = Integer.getInteger("kynet.broker.external.port", DEFAULT_BROKER_PORT);
        int senderThreads = positiveIntProperty("kynet.broker.maxQps.senderThreads",
                Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors())));
        int warmupSeconds = nonNegativeIntProperty("kynet.broker.maxQps.warmupSeconds", 3);
        int durationSeconds = positiveIntProperty("kynet.broker.maxQps.durationSeconds", 10);

        ServiceNode provider = new ServiceNode(host, port, "MaxOneWayServiceA");
        ServiceNode caller = new ServiceNode(host, port, "MaxOneWayServiceB");
        try {
            try {
                provider.start();
            } catch (RuntimeException e) {
                Assumptions.abort("外部 broker 不可达 " + host + ":" + port
                        + "，请先启动 broker。原因: " + rootCause(e));
            }

            new ActionBootstrap(provider).registerAction(new MaxOneWayAction());
            OneWayCounter counter = new OneWayCounter();
            provider.registerFastOneWayHandler(ONE_WAY_CMD, ONE_WAY_ACT, counter);
            caller.start();

            InvokeOptions options = InvokeOptions.builder()
                    .sessionId(202_002L)
                    .userId(1L)
                    .timeout(Duration.ofSeconds(3))
                    .build();
            waitForOneWayRoute(caller, options);

            if (warmupSeconds > 0) {
                OneWayResult warmup = runOneWayWindow(caller, options, senderThreads,
                        Duration.ofSeconds(warmupSeconds), counter);
                printOneWayResult("max one-way warmup", warmup);
            }

            OneWayResult measured = runOneWayWindow(caller, options, senderThreads,
                    Duration.ofSeconds(durationSeconds), counter);
            printOneWayResult("max one-way qps", measured);
            System.out.printf(
                    "max one-way transport: ipcSent=%d, udpSent=%d, dropped=%d, queuedBytes=%d%n",
                    caller.directIpcSentCount(), caller.directUdpSentCount(),
                    caller.directDroppedCount(), caller.directQueuedBytes());

            assertTrue(measured.received() > 0, "测量窗口内至少应有一帧 one-way 请求到达服务 A");
        } finally {
            caller.shutdown();
            provider.shutdown();
        }
    }

    private static void waitForOneWayRoute(ServiceNode caller, InvokeOptions options)
            throws InterruptedException {
        ServiceNode.FastOneWaySender probe =
                caller.prepareFastOneWaySender(ONE_WAY_CMD, ONE_WAY_ACT, null, options);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        RuntimeException lastException = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                probe.send();
                // The route is in the directory; give the provider a moment to finish any
                // pending registration work before starting the measurement.
                TimeUnit.MILLISECONDS.sleep(100);
                return;
            } catch (RuntimeException e) {
                lastException = e;
                TimeUnit.MILLISECONDS.sleep(25);
            }
        }
        fail("one-way action " + ONE_WAY_CMD + ":" + ONE_WAY_ACT
                + " 未就绪: " + (lastException == null ? "unknown" : lastException.getMessage()));
    }

    private static OneWayResult runOneWayWindow(ServiceNode caller, InvokeOptions options,
                                                int senderThreads, Duration duration,
                                                OneWayCounter counter) throws InterruptedException {
        long receivedBaseline = counter.count();
        LongAdder sent = new LongAdder();
        LongAdder failed = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);

        long deadlineNanos = System.nanoTime() + duration.toNanos();
        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastOneWaySender preparedSender =
                        caller.prepareFastOneWaySender(ONE_WAY_CMD, ONE_WAY_ACT, null, options);
                int batching = 0;
                try {
                    while (true) {
                        try {
                            preparedSender.send();
                            sent.increment();
                        } catch (RuntimeException e) {
                            failed.increment();
                            firstFailure.compareAndSet(null,
                                    e.getClass().getSimpleName() + ": " + e.getMessage());
                        }
                        if ((++batching & 63) == 0 && System.nanoTime() >= deadlineNanos) {
                            return;
                        }
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "max-oneway-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sendersDone.await();
        long received = counter.count() - receivedBaseline;
        double seconds = duration.toNanos() / 1_000_000_000.0;
        return new OneWayResult(senderThreads, seconds, sent.sum(), failed.sum(),
                received, firstFailure.get());
    }

    private static void printOneWayResult(String label, OneWayResult result) {
        System.out.printf(
                "%s: duration=%.2fs, senderThreads=%d, sent=%d, failed=%d, received=%d, "
                        + "sendQps=%.2f, receiveQps=%.2f%n",
                label,
                result.durationSeconds(),
                result.senderThreads(),
                result.sent(),
                result.failed(),
                result.received(),
                result.sendQps(),
                result.receiveQps());
        if (result.firstFailure() != null) {
            System.out.println(label + " firstFailure=" + result.firstFailure());
        }
    }

    private record OneWayResult(int senderThreads, double durationSeconds, long sent, long failed,
                                long received, String firstFailure) {
        private double sendQps() {
            return durationSeconds <= 0 ? 0 : sent / durationSeconds;
        }

        private double receiveQps() {
            return durationSeconds <= 0 ? 0 : received / durationSeconds;
        }
    }

    @ActionController(cmd = ONE_WAY_CMD)
    private static final class MaxOneWayAction {
        @ActionMethod(act = ONE_WAY_ACT, mode = ActionExecutionMode.INLINE)
        public void oneWay(ActionContext context) {
        }
    }

    private static final class OneWayCounter implements ServiceNode.FastOneWayHandler {
        private static final int ACTION = ActionKey.of(ONE_WAY_CMD, ONE_WAY_ACT);
        private final LongAdder received = new LongAdder();

        @Override
        public void onOneWay(int action, long requestId, long userId, int gatewayInstanceId,
                             DirectBuffer bodyBuffer, int bodyOffset, int bodyLength) {
            if (action == ACTION) {
                received.increment();
            }
        }

        private long count() {
            return received.sum();
        }
    }

    private static ThroughputResult runWindow(ServiceNode caller, InvokeOptions options,
                                              int senderThreads, int maxInFlight,
                                              Duration duration, MaxQpsAction action)
            throws InterruptedException {
        long providerBaseline = action.receivedCount();
        AtomicLong sent = new AtomicLong();
        AtomicLong success = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong inFlight = new AtomicLong();
        Map<String, AtomicLong> failureCounts = new ConcurrentHashMap<>();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastRpcSender preparedSender =
                        caller.prepareFastRpcSender(CMD, ACT, null, options);
                ServiceNode.FastRpcCallback callback = new ServiceNode.FastRpcCallback() {
                    @Override
                    public void onResponse(boolean ok, RpcResponseView response) {
                        inFlight.decrementAndGet();
                        if (System.nanoTime() > deadlineNanos) {
                            return;
                        }
                        if (ok) {
                            success.incrementAndGet();
                        } else {
                            failed.incrementAndGet();
                            recordFailure(failureCounts, firstFailure,
                                    response == null ? "UNKNOWN" : "ERROR:" + response.errorCode(),
                                    response == null ? null : response.errorMessage());
                        }
                    }

                    @Override
                    public void onFailure(String errorCode, String errorMessage) {
                        inFlight.decrementAndGet();
                        if (System.nanoTime() > deadlineNanos) {
                            return;
                        }
                        failed.incrementAndGet();
                        recordFailure(failureCounts, firstFailure, errorCode, errorMessage);
                    }
                };

                try {
                    while (System.nanoTime() < deadlineNanos) {
                        if (inFlight.incrementAndGet() > maxInFlight) {
                            inFlight.decrementAndGet();
                            Thread.onSpinWait();
                            continue;
                        }

                        sent.incrementAndGet();
                        try {
                            preparedSender.invoke(callback);
                        } catch (RuntimeException e) {
                            inFlight.decrementAndGet();
                            failed.incrementAndGet();
                            Throwable cause = rootCause(e);
                            recordFailure(failureCounts, firstFailure,
                                    "EXCEPTION:" + cause.getClass().getSimpleName(), cause.getMessage());
                        }
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "max-qps-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sendersDone.await();

        long drainDeadlineNanos = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(10) + options.getTimeout().toNanos();
        while (inFlight.get() > 0 && System.nanoTime() < drainDeadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(1);
        }

        long providerReceived = action.receivedCount() - providerBaseline;
        double seconds = duration.toNanos() / 1_000_000_000.0;
        return new ThroughputResult(senderThreads, maxInFlight, seconds,
                sent.get(), success.get(), failed.get(), inFlight.get(),
                providerReceived, Map.copyOf(failureCounts), firstFailure.get());
    }

    private static void waitForActionReady(ServiceNode caller, InvokeOptions options)
            throws InterruptedException {
        InvokeOptions probeOptions = options.toBuilder()
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        ActionResult lastResult = null;
        RuntimeException lastException = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                lastResult = caller.invoke(CMD, ACT, null, probeOptions);
                if (lastResult.isSuccess()) {
                    return;
                }
            } catch (RuntimeException e) {
                lastException = e;
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (lastException != null) {
            fail("Action " + CMD + ":" + ACT + " 未就绪: " + lastException.getMessage());
        }
        fail("Action " + CMD + ":" + ACT + " 未就绪, lastResult=" + describe(lastResult));
    }

    private static void printResult(String label, ThroughputResult result) {
        System.out.printf(
                "%s: duration=%.2fs, senderThreads=%d, maxInFlight=%d, sent=%d, success=%d, "
                        + "failed=%d, inFlightLeft=%d, sendQps=%.2f, successQps=%.2f, "
                        + "providerReceived=%d, providerQps=%.2f%n",
                label,
                result.durationSeconds(),
                result.senderThreads(),
                result.maxInFlight(),
                result.sent(),
                result.success(),
                result.failed(),
                result.inFlightLeft(),
                result.sendQps(),
                result.successQps(),
                result.providerReceived(),
                result.providerQps());
        if (!result.failureCounts().isEmpty()) {
            System.out.println(label + " failures=" + result.failureCounts()
                    + ", firstFailure=" + result.firstFailure());
        }
    }

    private static void recordFailure(Map<String, AtomicLong> failureCounts,
                                      AtomicReference<String> firstFailure,
                                      String errorCode, String errorMessage) {
        String code = errorCode == null || errorCode.isBlank() ? "UNKNOWN" : errorCode;
        failureCounts.computeIfAbsent(code, ignored -> new AtomicLong()).incrementAndGet();
        firstFailure.compareAndSet(null, code + ": " + errorMessage);
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
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

    /**
     * 按 direct 传输的每对端队列预算估计一个不会立刻反压的在途窗口，每帧按 512 字节保守估算。
     */
    private static int defaultMaxInFlight() {
        long peerQueueBytes = Long.getLong("kynet.direct.peerQueueBytes", 4L * 1024 * 1024);
        return (int) Math.max(512L, Math.min(65_536L, peerQueueBytes / 512L));
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
    }

    private static int nonNegativeIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value >= 0 ? value : defaultValue;
    }

    @ActionController(cmd = CMD)
    private static final class MaxQpsAction {
        private final AtomicLong received = new AtomicLong();

        @ActionMethod(act = ACT, mode = ActionExecutionMode.INLINE)
        public int ping(ActionContext context) {
            received.incrementAndGet();
            return 0;
        }


        private long receivedCount() {
            return received.get();
        }
    }

    private record ThroughputResult(int senderThreads, int maxInFlight, double durationSeconds,
                                    long sent, long success, long failed, long inFlightLeft,
                                    long providerReceived, Map<String, AtomicLong> failureCounts,
                                    String firstFailure) {
        private double sendQps() {
            return durationSeconds <= 0 ? 0 : sent / durationSeconds;
        }

        private double successQps() {
            return durationSeconds <= 0 ? 0 : success / durationSeconds;
        }

        private double providerQps() {
            return durationSeconds <= 0 ? 0 : providerReceived / durationSeconds;
        }
    }
}
