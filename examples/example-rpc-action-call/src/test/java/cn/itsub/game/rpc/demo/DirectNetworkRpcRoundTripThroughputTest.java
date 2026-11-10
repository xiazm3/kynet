package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.registry.RegistryServer;
import cn.itsub.kynet.common.transport.AeronTransport;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class DirectNetworkRpcRoundTripThroughputTest {
    private static final int CMD = 110;
    private static final int ACT = 1;
    private static final int DEFAULT_BROKER_PARTITIONS = 2;
    private static final int DEFAULT_SENDER_THREADS = 4;
    private static final int DEFAULT_MAX_IN_FLIGHT = 4_096;
    private static final int DEFAULT_WARMUP_SECONDS = 3;
    private static final int DEFAULT_DURATION_SECONDS = 10;

    @Test
    @Timeout(40)
    void measuresBrokerNetworkModeRpcRoundTripThroughput() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-broker-network-rpc-roundtrip-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        String previousBrokerPartitions = System.getProperty("kynet.broker.partitions");
        String previousReplyHost = System.getProperty("kynet.reply.host");

        String networkHost = stringProperty("kynet.broker.networkRpcBenchmark.host", AeronTransport.localHostAddress());
        assumeFalse(AeronTransport.isLocalHost(networkHost),
                "network RPC benchmark requires a non-IPC host; set -Dkynet.broker.networkRpcBenchmark.host=<nic-ip>");

        System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());
        System.setProperty("kynet.reply.host", networkHost);
        System.setProperty("kynet.broker.partitions", String.valueOf(
                positiveIntProperty("kynet.broker.networkRpcBenchmark.partitions", DEFAULT_BROKER_PARTITIONS)));

        int port = AeronTransport.findAvailablePort();
        String brokerDirectory = AeronTransport.brokerAeronDirectory(port);
        MediaDriver mediaDriver = null;
        RegistryServer brokerServer = null;
        ServiceNode serviceB = null;
        ServiceNode serviceA = null;
        try {
            mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .threadingMode(ThreadingMode.DEDICATED)
                    .aeronDirectoryName(brokerDirectory));

            brokerServer = new RegistryServer(port);
            brokerServer.start();

            RpcRoundTripAction action = new RpcRoundTripAction();
            serviceB = new ServiceNode(networkHost, port, "NetworkRpcServiceB");
            serviceB.start();
            new ActionBootstrap(serviceB).registerAction(action);

            serviceA = new ServiceNode(networkHost, port, "NetworkRpcServiceA");
            serviceA.start();

            InvokeOptions options = InvokeOptions.builder()
                    .targetInstanceId(serviceB.getInstanceId())
                    .sessionId(110_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(positiveIntProperty(
                            "kynet.broker.networkRpcBenchmark.timeoutMs", 3_000)))
                    .build();

            waitForProviderRegistration(serviceA, options);
            waitUntilReceiverQuiet(action, Duration.ofMillis(200), Duration.ofSeconds(3));

            int senderThreads = positiveIntProperty(
                    "kynet.broker.networkRpcBenchmark.senderThreads", DEFAULT_SENDER_THREADS);
            int maxInFlight = positiveIntProperty(
                    "kynet.broker.networkRpcBenchmark.maxInFlight", DEFAULT_MAX_IN_FLIGHT);
            int warmupSeconds = positiveIntProperty(
                    "kynet.broker.networkRpcBenchmark.warmupSeconds", DEFAULT_WARMUP_SECONDS);
            int durationSeconds = positiveIntProperty(
                    "kynet.broker.networkRpcBenchmark.durationSeconds", DEFAULT_DURATION_SECONDS);

            RoundTripResult warmup = runRpcRoundTripWindow(action, serviceA, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(warmupSeconds));
            printResult("broker network RPC round-trip warmup", networkHost, warmup);
            waitUntilReceiverQuiet(action, Duration.ofMillis(200), Duration.ofSeconds(3));

            RoundTripResult measured = runRpcRoundTripWindow(action, serviceA, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(durationSeconds));
            printResult("broker network RPC round-trip throughput", networkHost, measured);

            assertTrue(measured.aSuccessWindow() > 0,
                    "service A should receive at least one successful RPC response during the measured window");
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
            restoreProperty("kynet.reply.host", previousReplyHost);
            restoreProperty("kynet.aeron.baseDir", previousBaseDir);
        }
    }

    private static void waitForProviderRegistration(ServiceNode serviceA, InvokeOptions options)
            throws InterruptedException {
        InvokeOptions probeOptions = options.toBuilder()
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        ActionResult lastResult = null;
        RuntimeException lastException = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                lastResult = serviceA.invoke(CMD, ACT, null, probeOptions);
                if (lastResult.isSuccess()) {
                    return;
                }
            } catch (RuntimeException e) {
                lastException = e;
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        if (lastException != null) {
            fail("network RPC action 110:1 was not ready before benchmark: " + lastException.getMessage());
        }
        fail("network RPC action 110:1 was not ready before benchmark, lastResult=" + describe(lastResult));
    }

    private static RoundTripResult runRpcRoundTripWindow(RpcRoundTripAction action, ServiceNode serviceA,
                                                         InvokeOptions options, int senderThreads, int maxInFlight,
                                                         Duration duration) throws InterruptedException {
        RpcRoundTripCounters counters = new RpcRoundTripCounters();
        AtomicLong inFlight = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        long bTotalBefore = action.totalReceived();
        long startNanos = System.nanoTime();
        long endNanos = startNanos + duration.toNanos();
        action.startMeasurement(startNanos, endNanos);
        counters.startMeasurement(startNanos, endNanos);

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastRpcSender preparedSender =
                        serviceA.prepareFastRpcSender(CMD, ACT, null, options);
                try {
                    while (System.nanoTime() < endNanos) {
                        sendOne(preparedSender, counters, inFlight, maxInFlight);
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "broker-network-rpc-roundtrip-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        waitForSenders(sendersDone, senderThreads);
        long sendEndNanos = System.nanoTime();
        drainInFlight(inFlight, options.getTimeout());
        while (System.nanoTime() < action.measurementEndNanos()) {
            TimeUnit.MILLISECONDS.sleep(1);
        }

        double sendSeconds = Math.max(0.000001, (sendEndNanos - startNanos) / 1_000_000_000.0);
        double receiveWindowSeconds = duration.toNanos() / 1_000_000_000.0;
        return new RoundTripResult(
                AeronTransport.brokerInputPartitionCount(),
                senderThreads,
                maxInFlight,
                sendSeconds,
                receiveWindowSeconds,
                counters.sent.get(),
                counters.sendFailed.get(),
                counters.aSuccessWindow.get(),
                counters.aSuccessTotal.get(),
                counters.aFailedWindow.get(),
                counters.aFailedTotal.get(),
                action.windowReceived(),
                action.totalReceived() - bTotalBefore,
                inFlight.get());
    }

    private static void sendOne(ServiceNode.FastRpcSender sender, RpcRoundTripCounters counters,
                                AtomicLong inFlight, int maxInFlight) {
        long currentInFlight = inFlight.incrementAndGet();
        if (currentInFlight > maxInFlight) {
            inFlight.decrementAndGet();
            Thread.onSpinWait();
            return;
        }

        counters.sent.incrementAndGet();
        try {
            CompletableFuture<ActionResult> future = sender.invokeAsync();
            future.whenComplete((result, throwable) -> {
                long now = System.nanoTime();
                inFlight.decrementAndGet();
                boolean inWindow = counters.isInWindow(now);
                if (throwable != null || result == null || !result.isSuccess()) {
                    counters.aFailedTotal.incrementAndGet();
                    if (inWindow) {
                        counters.aFailedWindow.incrementAndGet();
                    }
                    return;
                }
                counters.aSuccessTotal.incrementAndGet();
                if (inWindow) {
                    counters.aSuccessWindow.incrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            long now = System.nanoTime();
            inFlight.decrementAndGet();
            counters.sendFailed.incrementAndGet();
            counters.aFailedTotal.incrementAndGet();
            if (counters.isInWindow(now)) {
                counters.aFailedWindow.incrementAndGet();
            }
        }
    }

    private static void waitForSenders(CountDownLatch sendersDone, int senderThreads) throws InterruptedException {
        if (!sendersDone.await(1, TimeUnit.DAYS)) {
            fail("sender threads did not finish, senderThreads=" + senderThreads);
        }
    }

    private static void drainInFlight(AtomicLong inFlight, Duration timeout) throws InterruptedException {
        long drainDeadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos() + timeout.toNanos();
        while (inFlight.get() > 0 && System.nanoTime() < drainDeadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(2);
        }
    }

    private static void waitUntilReceiverQuiet(RpcRoundTripAction action, Duration quietFor, Duration timeout)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        long quietStartNanos = System.nanoTime();
        long lastTotal = action.totalReceived();
        while (System.nanoTime() < deadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(20);
            long currentTotal = action.totalReceived();
            long now = System.nanoTime();
            if (currentTotal == lastTotal) {
                if (now - quietStartNanos >= quietFor.toNanos()) {
                    return;
                }
                continue;
            }
            lastTotal = currentTotal;
            quietStartNanos = now;
        }
    }

    private static void printResult(String label, String host, RoundTripResult result) {
        System.out.printf(
                "%s: mode=udp, path=A->broker->B->broker->A, host=%s, partitions=%d, senderThreads=%d, maxInFlight=%d, sendDuration=%.2fs, receiveWindow=%.2fs, sent=%d, sendFailed=%d, aSuccessWindow=%d, aSuccessTotal=%d, aFailedWindow=%d, aFailedTotal=%d, bWindowReceived=%d, bTotalReceived=%d, inFlightLeft=%d, sendQps=%.2f, aRpcSuccessQps=%.2f, bReceiveQps=%.2f%n",
                label,
                host,
                result.partitions(),
                result.senderThreads(),
                result.maxInFlight(),
                result.sendSeconds(),
                result.receiveWindowSeconds(),
                result.sent(),
                result.sendFailed(),
                result.aSuccessWindow(),
                result.aSuccessTotal(),
                result.aFailedWindow(),
                result.aFailedTotal(),
                result.bWindowReceived(),
                result.bTotalReceived(),
                result.inFlightLeft(),
                result.sendQps(),
                result.aRpcSuccessQps(),
                result.bReceiveQps());
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
    }

    private static String stringProperty(String name, String defaultValue) {
        String value = System.getProperty(name);
        return value == null || value.isBlank() ? defaultValue : value;
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

    private static void restoreProperty(String name, String previousValue) {
        if (previousValue == null) {
            System.clearProperty(name);
            return;
        }
        System.setProperty(name, previousValue);
    }

    @ActionController(cmd = CMD)
    private static final class RpcRoundTripAction {
        private final AtomicLong totalReceived = new AtomicLong();
        private final AtomicLong windowReceived = new AtomicLong();
        private final AtomicLong measurementStartNanos = new AtomicLong();
        private final AtomicLong measurementEndNanos = new AtomicLong();

        @ActionMethod(act = ACT, mode = ActionExecutionMode.INLINE)
        public ActionResult invoke(ActionContext context) {
            recordReceived();
            return ActionResult.success(null);
        }

        private void recordReceived() {
            totalReceived.incrementAndGet();
            long now = System.nanoTime();
            long start = measurementStartNanos.get();
            long end = measurementEndNanos.get();
            if (start != 0L && now >= start && now < end) {
                windowReceived.incrementAndGet();
            }
        }

        private void startMeasurement(long startNanos, long endNanos) {
            windowReceived.set(0);
            measurementStartNanos.set(startNanos);
            measurementEndNanos.set(endNanos);
        }

        private long totalReceived() {
            return totalReceived.get();
        }

        private long windowReceived() {
            return windowReceived.get();
        }

        private long measurementEndNanos() {
            return measurementEndNanos.get();
        }
    }

    private static final class RpcRoundTripCounters {
        private final AtomicLong sent = new AtomicLong();
        private final AtomicLong sendFailed = new AtomicLong();
        private final AtomicLong aSuccessWindow = new AtomicLong();
        private final AtomicLong aSuccessTotal = new AtomicLong();
        private final AtomicLong aFailedWindow = new AtomicLong();
        private final AtomicLong aFailedTotal = new AtomicLong();
        private final AtomicLong measurementStartNanos = new AtomicLong();
        private final AtomicLong measurementEndNanos = new AtomicLong();

        private void startMeasurement(long startNanos, long endNanos) {
            measurementStartNanos.set(startNanos);
            measurementEndNanos.set(endNanos);
        }

        private boolean isInWindow(long nowNanos) {
            long start = measurementStartNanos.get();
            long end = measurementEndNanos.get();
            return start != 0L && nowNanos >= start && nowNanos < end;
        }
    }

    private record RoundTripResult(int partitions, int senderThreads, int maxInFlight,
                                   double sendSeconds, double receiveWindowSeconds,
                                   long sent, long sendFailed,
                                   long aSuccessWindow, long aSuccessTotal,
                                   long aFailedWindow, long aFailedTotal,
                                   long bWindowReceived, long bTotalReceived,
                                   long inFlightLeft) {
        private double sendQps() {
            return sendSeconds <= 0 ? 0 : sent / sendSeconds;
        }

        private double aRpcSuccessQps() {
            return receiveWindowSeconds <= 0 ? 0 : aSuccessWindow / receiveWindowSeconds;
        }

        private double bReceiveQps() {
            return receiveWindowSeconds <= 0 ? 0 : bWindowReceived / receiveWindowSeconds;
        }
    }
}
