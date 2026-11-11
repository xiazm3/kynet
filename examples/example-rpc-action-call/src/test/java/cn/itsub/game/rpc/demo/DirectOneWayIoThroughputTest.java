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
import cn.itsub.kynet.common.util.ActionKey;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class DirectOneWayIoThroughputTest {
    private static final int CMD = 103;
    private static final int ACT = 1;
    private static final int ACTION = ActionKey.of(CMD, ACT);

    @Test
    @Timeout(30)
    void measuresOneWayPacketsPerSecondBetweenServices() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-oneway-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());

        int port = AeronTransport.findAvailablePort();
        String brokerDirectory = AeronTransport.brokerAeronDirectory(port);
        MediaDriver mediaDriver = null;
        RegistryServer brokerServer = null;
        ServiceNode receiver = null;
        ServiceNode sender = null;
        try {
            mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .threadingMode(ThreadingMode.DEDICATED)
                    .aeronDirectoryName(brokerDirectory));

            brokerServer = new RegistryServer(port);
            brokerServer.start();

            CountingActionBase action = Boolean.getBoolean("kynet.broker.ioBenchmark.inlineReceiver")
                    ? new InlineCountingOneWayAction()
                    : new AsyncCountingOneWayAction();
            receiver = new ServiceNode("127.0.0.1", port, "OneWayIoReceiver");
            receiver.start();
            if (Boolean.getBoolean("kynet.broker.ioBenchmark.fastReceiver")) {
                receiver.setFastOneWayHandler((actionKey, requestId, userId, gatewayInstanceId,
                                               bodyBuffer, bodyOffset, bodyLength) -> {
                    if (actionKey == ACTION) {
                        action.recordReceived();
                    }
                });
            }
            new ActionBootstrap(receiver).registerAction(action);

            sender = new ServiceNode("127.0.0.1", port, "OneWayIoSender");
            sender.start();

            InvokeOptions options = InvokeOptions.builder()
                    .sessionId(100_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(positiveIntProperty("kynet.broker.ioBenchmark.timeoutMs", 3_000)))
                    .build();

            waitForProviderRegistration(sender);
            waitUntilReceiverQuiet(action, Duration.ofMillis(200), Duration.ofSeconds(3));

            int senderThreads = positiveIntProperty("kynet.broker.ioBenchmark.senderThreads", 4);
            int warmupSeconds = nonNegativeIntProperty("kynet.broker.ioBenchmark.warmupSeconds", 1);
            int durationSeconds = positiveIntProperty("kynet.broker.ioBenchmark.durationSeconds", 3);
            boolean fastSender = Boolean.getBoolean("kynet.broker.ioBenchmark.fastSender");

            if (warmupSeconds > 0) {
                ThroughputResult warmup = runOneWayWindow(action, sender, options, senderThreads,
                        Duration.ofSeconds(warmupSeconds), fastSender);
                printResult("broker one-way warmup", warmup);
                waitUntilReceiverQuiet(action, Duration.ofMillis(200), Duration.ofSeconds(3));
            }

            ThroughputResult measured = runOneWayWindow(action, sender, options, senderThreads,
                    Duration.ofSeconds(durationSeconds), fastSender);
            printResult("broker one-way io throughput", measured);

            assertTrue(measured.received() > 0,
                    "receiver should observe at least one one-way packet during the measured window");
        } finally {
            if (sender != null) {
                sender.shutdown();
            }
            if (receiver != null) {
                receiver.shutdown();
            }
            if (brokerServer != null) {
                brokerServer.stop();
            }
            if (mediaDriver != null) {
                mediaDriver.close();
            }
            restoreProperty("kynet.aeron.baseDir", previousBaseDir);
        }
    }

    private static void waitForProviderRegistration(ServiceNode sender) throws InterruptedException {
        InvokeOptions probeOptions = InvokeOptions.builder()
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
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
            fail("one-way action 103:1 was not ready before benchmark: " + lastException.getMessage());
        }
        fail("one-way action 103:1 was not ready before benchmark, lastResult=" + describe(lastResult));
    }

    private static ThroughputResult runOneWayWindow(CountingActionBase action, ServiceNode sender,
                                                    InvokeOptions options, int senderThreads,
                                                    Duration duration, boolean fastSender) throws InterruptedException {
        action.startMeasurement(duration);

        LongAdder sent = new LongAdder();
        LongAdder failed = new LongAdder();
        AtomicReference<String> firstFailure = new AtomicReference<>();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastOneWaySender preparedSender = fastSender
                        ? sender.prepareFastOneWaySender(CMD, ACT, null, options)
                        : null;
                try {
                    int batching = 0;
                    while (true) {
                        try {
                            if (preparedSender == null) {
                                sender.invokeOneWay(CMD, ACT, null, options);
                            } else {
                                preparedSender.send();
                            }
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
            }, "broker-oneway-io-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sendersDone.await();
        long endNanos = System.nanoTime();
        long measurementEndNanos = action.measurementEndNanos();
        while (System.nanoTime() < measurementEndNanos) {
            TimeUnit.MILLISECONDS.sleep(1);
        }
        action.stopMeasurement();

        double sendSeconds = Math.max(0.000001, (endNanos - startNanos) / 1_000_000_000.0);
        double receiveSeconds = duration.toNanos() / 1_000_000_000.0;
        return new ThroughputResult(
                senderThreads,
                sendSeconds,
                receiveSeconds,
                sent.sum(),
                failed.sum(),
                action.measuredReceived(),
                firstFailure.get());
    }

    private static void waitUntilReceiverQuiet(CountingActionBase action, Duration quietFor, Duration timeout)
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

    private static void printResult(String label, ThroughputResult result) {
        System.out.printf(
                "%s: sendDuration=%.2fs, receiveWindow=%.2fs, senderThreads=%d, sent=%d, failed=%d, received=%d, sendQps=%.2f, receiveQps=%.2f%n",
                label,
                result.sendSeconds(),
                result.receiveSeconds(),
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

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
    }

    private static int nonNegativeIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value >= 0 ? value : defaultValue;
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

    private abstract static class CountingActionBase {
        private final LongAdder totalReceived = new LongAdder();
        private final AtomicLong measurementEndNanos = new AtomicLong();
        private long measuredStart;

        protected void recordReceived() {
            totalReceived.increment();
        }

        private void startMeasurement(Duration duration) {
            measuredStart = totalReceived.sum();
            measurementEndNanos.set(System.nanoTime() + duration.toNanos());
        }

        private void stopMeasurement() {
        }

        private long totalReceived() {
            return totalReceived.sum();
        }

        private long measuredReceived() {
            return totalReceived.sum() - measuredStart;
        }

        private long measurementEndNanos() {
            return measurementEndNanos.get();
        }
    }

    @ActionController(CMD)
    private static final class AsyncCountingOneWayAction extends CountingActionBase {
        @ActionMethod(ACT)
        public void receive(ActionContext context) {
            recordReceived();
        }
    }

    @ActionController(CMD)
    private static final class InlineCountingOneWayAction extends CountingActionBase {
        @ActionMethod(value = ACT, mode = ActionExecutionMode.INLINE)
        public void receive(ActionContext context) {
            recordReceived();
        }
    }

    private record ThroughputResult(int senderThreads, double sendSeconds, double receiveSeconds,
                                    long sent, long failed, long received, String firstFailure) {
        private double sendQps() {
            return sent / sendSeconds;
        }

        private double receiveQps() {
            return receiveSeconds <= 0 ? 0 : received / receiveSeconds;
        }
    }
}
