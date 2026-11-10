package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.registry.RegistryServer;
import cn.itsub.kynet.common.transport.AeronTransport;
import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.ActionExecutionMode;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 服务 A 向服务 B 连续发送 one-way 消息的极限吞吐测试。
 *
 * <p>拓扑：Broker（只做注册/发现）+ ServiceA（发送方）+ ServiceB（接收方）。
 * A 通过直连传输（不经过 Broker）把消息直接发给 B。
 *
 * <p>本测试回答两个问题：
 * <ul>
 *   <li>A 每秒最多能发出多少条消息（sendQps，以本地传输接收为准）；</li>
 *   <li>B 实际收到多少条（receiveQps、投递率、丢弃数）。</li>
 * </ul>
 *
 * <p>本机 {@code 127.0.0.1} 会自动走 Aeron IPC。把 host 设成本机网卡地址时，只要对端
 * 的 IPC marker 可达，仍然优先走 IPC；要测真正的 UDP 路径，需要额外加
 * {@code -Dkynet.direct.ipc.enabled=false}。每轮结果里的 ipcSent/udpSent 会显示实际使用的传输。
 *
 * <p>可调参数（JVM -D）：
 * <pre>
 *   kynet.abPeak.host            目标地址，默认 127.0.0.1（IPC）
 *   kynet.abPeak.senderThreads   发送线程数，支持逗号列表做扫描，默认 4
 *   kynet.abPeak.payloadBytes    每条消息 body 字节数，默认 64，0 表示无 payload
 *   kynet.abPeak.warmupSeconds   每轮预热秒数，默认 2
 *   kynet.abPeak.durationSeconds 每轮测量秒数，默认 5
 *   kynet.abPeak.sender          fast（预编码快速发送器）| generic（普通 invokeOneWay），默认 fast
 *   kynet.abPeak.receiver        fast（FastOneWayHandler）| inline | async | noop，默认 fast
 *   kynet.direct.termLength      直接传输 term 长度，默认 8MB；极限吞吐建议 32-64MB
 * </pre>
 *
 * <p>注意：所有发送线程共享每个 peer 的一条 publication，单线程通常最快；多线程会在
 * ConcurrentPublication 的原子 claim 上互相竞争，2-4 线程反而可能更慢。
 *
 * <p>示例（扫描 1/2/4/8/16 线程，单轮 10 秒）：
 * <pre>
 *   mvn -pl kynet-examples/example-rpc-action-call -am \
 *       -Dtest=ABOneWayPeakThroughputTest -Dsurefire.failIfNoSpecifiedTests=false \
 *       "-Dkynet.abPeak.senderThreads=1,2,4,8,16" \
 *       "-Dkynet.abPeak.durationSeconds=10" test
 * </pre>
 *
 * <p>强制 UDP（同一台机器上绕过 IPC）：
 * <pre>
 *   ... "-Dkynet.abPeak.host=&lt;本机网卡地址&gt;" "-Dkynet.direct.ipc.enabled=false" ...
 * </pre>
 *
 * <p>注意：sendQps 统计的是“本地传输已接受”，不等于 B 已执行；判断 B 的真实能力要看
 * receiveQps、投递率和 B 侧的丢弃数。接收侧跟不上时 A 的发送队列会丢消息，这是极限压测
 * 的正常现象，因此本测试只断言有流量，不对投递率做门禁。
 */
class ABOneWayPeakThroughputTest {
    private static final int CMD = 121;
    private static final int ACT = 1;
    private static final int ACTION = ActionKey.of(CMD, ACT);
    private static final long SAMPLE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

    @Test
    @Timeout(600)
    void measuresPeakOneWayMessageRateFromServiceAToServiceB() throws Exception {
        String host = stringProperty("kynet.abPeak.host", "127.0.0.1");
        boolean loopback = AeronTransport.isLocalHost(host);
        int payloadBytes = nonNegativeIntProperty("kynet.abPeak.payloadBytes", 64);
        int[] threadCounts = parseIntList(System.getProperty("kynet.abPeak.senderThreads", "4"));
        int warmupSeconds = nonNegativeIntProperty("kynet.abPeak.warmupSeconds", 2);
        int durationSeconds = positiveIntProperty("kynet.abPeak.durationSeconds", 5);
        SendMode sendMode = SendMode.parse(stringProperty("kynet.abPeak.sender", "fast"));
        ReceiveMode receiveMode = ReceiveMode.parse(stringProperty("kynet.abPeak.receiver", "fast"));

        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-ab-peak-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        String previousReplyHost = System.getProperty("kynet.reply.host");
        System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());
        if (!loopback) {
            System.setProperty("kynet.reply.host", host);
        }

        int port = AeronTransport.findAvailablePort();
        MediaDriver mediaDriver = null;
        RegistryServer brokerServer = null;
        ServiceNode serviceB = null;
        ServiceNode serviceA = null;
        try {
            mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .threadingMode(ThreadingMode.DEDICATED)
                    .aeronDirectoryName(AeronTransport.brokerAeronDirectory(port)));
            brokerServer = new RegistryServer(port);
            brokerServer.start();

            ReceiverCounters counters = new ReceiverCounters(payloadBytes);
            serviceB = new ServiceNode(host, port, "ServiceB");
            serviceB.start();
            // 动作注册同时用于目录发现和就绪探测；FAST 模式下 one-way 帧会先被
            // fast handler 截获，不会走到这个动作方法。
            ActionBootstrap bootstrap = new ActionBootstrap(serviceB);
            if (receiveMode == ReceiveMode.FAST) {
                bootstrap.registerAction(new NoOpReceiverAction());
                serviceB.registerFastOneWayHandler(ACTION,
                        (action, requestId, userId, gatewayInstanceId, bodyBuffer, bodyOffset, bodyLength) ->
                                counters.record(bodyLength));
            } else if (receiveMode == ReceiveMode.NOOP) {
                bootstrap.registerAction(new NoOpReceiverAction());
                serviceB.registerFastOneWayHandler(ACTION,
                        (action, requestId, userId, gatewayInstanceId, bodyBuffer, bodyOffset, bodyLength) -> {
                            // Diagnostic only: consume the frame without touching the counters.
                        });
            } else if (receiveMode == ReceiveMode.INLINE) {
                bootstrap.registerAction(new InlineCountingReceiverAction(counters));
            } else {
                bootstrap.registerAction(new AsyncCountingReceiverAction(counters));
            }

            serviceA = new ServiceNode(host, port, "ServiceA");
            serviceA.start();

            System.out.printf(
                    "AB one-way peak: host=%s (%s), payloadBytes=%d, sender=%s, receiver=%s, threadCounts=%s, warmup=%ds, duration=%ds%n",
                    host, loopback ? "loopback, IPC preferred" : "direct IPC-or-UDP by peer marker",
                    payloadBytes, sendMode, receiveMode,
                    java.util.Arrays.toString(threadCounts), warmupSeconds, durationSeconds);

            waitForReceiverReady(serviceA);
            waitUntilQuiet(counters, Duration.ofMillis(200), Duration.ofSeconds(3));

            byte[] payload = payloadBytes > 0 ? new byte[payloadBytes] : null;
            for (int senderThreads : threadCounts) {
                if (warmupSeconds > 0) {
                    RunResult warmup = runWindow(serviceA, serviceB.getInstanceId(), counters,
                            senderThreads, payload, sendMode, Duration.ofSeconds(warmupSeconds));
                    printResult("warmup", warmup);
                    waitUntilQuiet(counters, Duration.ofMillis(200), Duration.ofSeconds(3));
                }

                RunResult measured = runWindow(serviceA, serviceB.getInstanceId(), counters,
                        senderThreads, payload, sendMode, Duration.ofSeconds(durationSeconds));
                printResult("measure", measured);
                printTimeline(measured);
                waitUntilQuiet(counters, Duration.ofMillis(200), Duration.ofSeconds(3));

                assertTrue(measured.sent() > 0, "A should send at least one message");
                if (receiveMode != ReceiveMode.NOOP) {
                    assertTrue(measured.received() > 0, "B should receive at least one message");
                }
                if (sendMode == SendMode.FAST && receiveMode == ReceiveMode.FAST && payloadBytes > 0) {
                    assertEquals(0, counters.badLength(), "B observed a body whose length differs from payloadBytes");
                }
            }
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
            restoreProperty("kynet.aeron.baseDir", previousBaseDir);
            restoreProperty("kynet.reply.host", previousReplyHost);
        }
    }

    private static RunResult runWindow(ServiceNode serviceA, int targetInstanceId,
                                       ReceiverCounters counters, int senderThreads, byte[] payload,
                                       SendMode sendMode, Duration duration) throws InterruptedException {
        long receivedBase = counters.received();
        long droppedBase = serviceA.directDroppedCount();
        long ipcSentBase = serviceA.directIpcSentCount();
        long udpSentBase = serviceA.directUdpSentCount();
        long retryBase = serviceA.directOfferRetryCount();
        LongAdder sent = new LongAdder();
        LongAdder failed = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        AtomicBoolean sampling = new AtomicBoolean(true);
        List<TimelineSample> samples = java.util.Collections.synchronizedList(new ArrayList<>());

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();

        Thread sampler = new Thread(() -> {
            long previousNanos = startNanos;
            long previousSent = 0;
            long previousReceived = receivedBase;
            while (sampling.get()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(TimeUnit.NANOSECONDS.toMillis(SAMPLE_INTERVAL_NANOS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                long now = System.nanoTime();
                long currentSent = sent.sum();
                long currentReceived = counters.received();
                samples.add(new TimelineSample(now - startNanos, currentSent - previousSent,
                        currentReceived - previousReceived, now - previousNanos));
                previousNanos = now;
                previousSent = currentSent;
                previousReceived = currentReceived;
            }
        }, "ab-peak-sampler");
        sampler.setDaemon(true);
        sampler.start();

        for (int i = 0; i < senderThreads; i++) {
            int threadIndex = i;
            Thread thread = new Thread(() -> {
                InvokeOptions options = InvokeOptions.builder()
                        .targetInstanceId(targetInstanceId)
                        .sessionId(121_000L + threadIndex)
                        .userId(threadIndex + 1L)
                        .timeout(Duration.ofSeconds(3))
                        .build();
                ServiceNode.FastOneWaySender prepared = sendMode == SendMode.FAST
                        ? serviceA.prepareFastOneWaySender(CMD, ACT, payload, options)
                        : null;
                int batch = 0;
                try {
                    while (true) {
                        try {
                            if (prepared == null) {
                                serviceA.invokeOneWay(CMD, ACT, payload, options);
                            } else {
                                prepared.send();
                            }
                            sent.increment();
                        } catch (RuntimeException e) {
                            // 背压/丢队列在极限压测中是正常现象：记录后继续发，
                            // 保持整个测量窗口固定，sendQps 才可比。
                            failed.increment();
                            firstFailure.compareAndSet(null,
                                    e.getClass().getSimpleName() + ": " + e.getMessage());
                        }
                        if ((++batch & 63) == 0 && System.nanoTime() >= deadlineNanos) {
                            return;
                        }
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "ab-peak-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sendersDone.await();
        long sendEndNanos = System.nanoTime();
        sampling.set(false);
        sampler.join(1_000);

        // 等待接收端把在途消息消费完，避免把 send 窗口末尾的在途消息漏算。
        long drainEndNanos = waitUntilQuiet(counters, Duration.ofMillis(200), Duration.ofSeconds(5));

        long received = counters.received() - receivedBase;
        long dropped = serviceA.directDroppedCount() - droppedBase;
        long ipcSent = serviceA.directIpcSentCount() - ipcSentBase;
        long udpSent = serviceA.directUdpSentCount() - udpSentBase;
        long retries = serviceA.directOfferRetryCount() - retryBase;
        double sendSeconds = Math.max(0.000001, (sendEndNanos - startNanos) / 1_000_000_000.0);
        double drainSeconds = Math.max(0.0, (drainEndNanos - sendEndNanos) / 1_000_000_000.0);
        return new RunResult(senderThreads, sendSeconds, drainSeconds, sent.sum(), failed.sum(),
                received, dropped, ipcSent, udpSent, retries, serviceA.directQueuedBytes(),
                firstFailure.get(), List.copyOf(samples));
    }

    private static void printResult(String label, RunResult result) {
        System.out.printf(
                "%s: threads=%d sendDuration=%.2fs drain=%.2fs sent=%d failed=%d received=%d dropped=%d "
                        + "sendQps=%.0f receiveQps=%.0f delivery=%.2f%% ipcSent=%d udpSent=%d retries=%d queuedBytes=%d%n",
                label,
                result.senderThreads(),
                result.sendSeconds(),
                result.drainSeconds(),
                result.sent(),
                result.failed(),
                result.received(),
                result.dropped(),
                result.sendQps(),
                result.receiveQps(),
                result.deliveryPercent(),
                result.ipcSent(),
                result.udpSent(),
                result.retries(),
                result.queuedBytes());
        if (result.firstFailure() != null) {
            System.out.println(label + " firstFailure=" + result.firstFailure());
        }
    }

    private static void printTimeline(RunResult result) {
        for (TimelineSample sample : result.samples()) {
            double seconds = sample.elapsedNanos() / 1_000_000_000.0;
            double intervalSeconds = Math.max(0.000001, sample.intervalNanos() / 1_000_000_000.0);
            System.out.printf("  t=%5.1fs sendQps=%.0f receiveQps=%.0f%n",
                    seconds, sample.sent() / intervalSeconds, sample.received() / intervalSeconds);
        }
    }

    private static void waitForReceiverReady(ServiceNode sender) throws InterruptedException {
        InvokeOptions probeOptions = InvokeOptions.builder()
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        ActionResult lastResult = null;
        RuntimeException lastException = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                lastResult = sender.invoke(CMD, ACT, null, probeOptions);
                if (lastResult.isSuccess()) {
                    return;
                }
            } catch (RuntimeException e) {
                lastException = e;
            }
            Thread.sleep(25);
        }
        if (lastException != null) {
            fail("action 121:1 was not ready before benchmark: " + lastException.getMessage());
        }
        fail("action 121:1 was not ready before benchmark, lastResult="
                + (lastResult == null ? "null" : lastResult.getErrorCode() + ":" + lastResult.getErrorMessage()));
    }

    private static long waitUntilQuiet(ReceiverCounters counters, Duration quietFor, Duration timeout)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        long quietStartNanos = System.nanoTime();
        long lastTotal = counters.received();
        while (System.nanoTime() < deadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(20);
            long currentTotal = counters.received();
            long now = System.nanoTime();
            if (currentTotal == lastTotal) {
                if (now - quietStartNanos >= quietFor.toNanos()) {
                    return now;
                }
                continue;
            }
            lastTotal = currentTotal;
            quietStartNanos = now;
        }
        return System.nanoTime();
    }

    private static String stringProperty(String name, String defaultValue) {
        String value = System.getProperty(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
    }

    private static int nonNegativeIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value >= 0 ? value : defaultValue;
    }

    private static int[] parseIntList(String value) {
        String[] parts = value.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            int parsed = Integer.parseInt(parts[i].trim());
            if (parsed <= 0) {
                throw new IllegalArgumentException("sender thread count must be positive: " + value);
            }
            result[i] = parsed;
        }
        return result;
    }

    private static void restoreProperty(String name, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, previousValue);
        }
    }

    private enum SendMode {
        FAST, GENERIC;

        static SendMode parse(String value) {
            return switch (value.toLowerCase(java.util.Locale.ROOT)) {
                case "fast" -> FAST;
                case "generic" -> GENERIC;
                default -> throw new IllegalArgumentException("unknown kynet.abPeak.sender: " + value);
            };
        }
    }

    private enum ReceiveMode {
        FAST, NOOP, INLINE, ASYNC;

        static ReceiveMode parse(String value) {
            return switch (value.toLowerCase(java.util.Locale.ROOT)) {
                case "fast" -> FAST;
                case "noop" -> NOOP;
                case "inline" -> INLINE;
                case "async" -> ASYNC;
                default -> throw new IllegalArgumentException("unknown kynet.abPeak.receiver: " + value);
            };
        }
    }

    private static final class ReceiverCounters {
        private final int expectedLength;
        private final LongAdder received = new LongAdder();
        private final LongAdder badLength = new LongAdder();

        private ReceiverCounters(int expectedLength) {
            this.expectedLength = expectedLength;
        }

        private void record(int bodyLength) {
            received.increment();
            if (expectedLength > 0 && bodyLength != expectedLength) {
                badLength.increment();
            }
        }

        private long received() {
            return received.sum();
        }

        private long badLength() {
            return badLength.sum();
        }
    }

    @ActionController(cmd = CMD)
    private static final class NoOpReceiverAction {
        @ActionMethod(act = ACT)
        public void onMessage(ActionContext context) {
            // FAST 模式下 one-way 帧由 fast handler 处理；这里只服务注册和 RPC 就绪探测。
        }
    }

    @ActionController(cmd = CMD)
    private static final class InlineCountingReceiverAction {
        private final ReceiverCounters counters;

        private InlineCountingReceiverAction(ReceiverCounters counters) {
            this.counters = counters;
        }

        @ActionMethod(act = ACT, mode = ActionExecutionMode.INLINE)
        public void onMessage(ActionContext context) {
            counters.record(-1);
        }
    }

    @ActionController(cmd = CMD)
    private static final class AsyncCountingReceiverAction {
        private final ReceiverCounters counters;

        private AsyncCountingReceiverAction(ReceiverCounters counters) {
            this.counters = counters;
        }

        @ActionMethod(act = ACT)
        public void onMessage(ActionContext context) {
            counters.record(-1);
        }
    }

    private record TimelineSample(long elapsedNanos, long sent, long received, long intervalNanos) {
    }

    private record RunResult(int senderThreads, double sendSeconds, double drainSeconds, long sent,
                             long failed, long received, long dropped, long ipcSent, long udpSent,
                             long retries, long queuedBytes, String firstFailure,
                             List<TimelineSample> samples) {
        private double sendQps() {
            return sent / sendSeconds;
        }

        private double receiveQps() {
            return received / (sendSeconds + drainSeconds);
        }

        private double deliveryPercent() {
            return sent == 0 ? 0.0 : received * 100.0 / sent;
        }
    }
}
