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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ServiceNodeDirectQpsTest {
    private static final int CMD = 105;
    private static final int ACT = 1;
    private static final int ACTION = ActionKey.of(CMD, ACT);
    private static final int DEFAULT_BROKER_PARTITIONS = 2;
    private static final int DEFAULT_SENDER_THREADS = 4;
    private static final int DEFAULT_DURATION_SECONDS = 10;

    @Test
    @Timeout(40)
    void measuresBrokerForwardPacketsPerSecondBetweenServiceNodes() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-client-forward-qps-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        String previousBrokerPartitions = System.getProperty("kynet.broker.partitions");
        System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());
        if (previousBrokerPartitions == null || previousBrokerPartitions.isBlank()) {
            System.setProperty("kynet.broker.partitions", String.valueOf(
                    positiveIntProperty("kynet.broker.forwardQps.partitions", DEFAULT_BROKER_PARTITIONS)));
        }

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

            ForwardCountingAction action = new ForwardCountingAction();
            receiver = new ServiceNode("127.0.0.1", port, "ForwardQpsReceiver");
            receiver.start();
            receiver.registerFastOneWayHandler(ACTION,
                    (actionKey, requestId, userId, gatewayInstanceId, bodyBuffer, bodyOffset, bodyLength) ->
                            action.recordReceived());
            new ActionBootstrap(receiver).registerAction(action);

            sender = new ServiceNode("127.0.0.1", port, "ForwardQpsSender");
            sender.start();

            InvokeOptions options = InvokeOptions.builder()
                    .targetInstanceId(receiver.getInstanceId())
                    .sessionId(105_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(positiveIntProperty("kynet.broker.forwardQps.timeoutMs", 3_000)))
                    .build();

            waitForProviderRegistration(sender, options);
            waitUntilReceiverQuiet(action, Duration.ofMillis(200), Duration.ofSeconds(3));

            int senderThreads = positiveIntProperty("kynet.broker.forwardQps.senderThreads",
                    DEFAULT_SENDER_THREADS);
            int durationSeconds = positiveIntProperty("kynet.broker.forwardQps.durationSeconds",
                    DEFAULT_DURATION_SECONDS);

            ThroughputResult measured = runForwardWindow(action, sender, options, senderThreads,
                    Duration.ofSeconds(durationSeconds));
            printResult("broker client forward qps", measured);

            assertTrue(measured.received() > 0,
                    "receiver should observe at least one forwarded packet during the measured window");
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
            restoreProperty("kynet.broker.partitions", previousBrokerPartitions);
            restoreProperty("kynet.aeron.baseDir", previousBaseDir);
        }
    }

    private static void waitForProviderRegistration(ServiceNode sender, InvokeOptions options)
            throws InterruptedException {
        InvokeOptions probeOptions = options.toBuilder()
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
            fail("forward action 105:1 was not ready before benchmark: " + lastException.getMessage());
        }
        fail("forward action 105:1 was not ready before benchmark, lastResult=" + describe(lastResult));
    }

    private static ThroughputResult runForwardWindow(ForwardCountingAction action, ServiceNode sender,
                                                     InvokeOptions options, int senderThreads, Duration duration)
            throws InterruptedException {
        action.startMeasurement(duration);

        AtomicLong sent = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastOneWaySender preparedSender =
                        sender.prepareFastOneWaySender(CMD, ACT, null, options);
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        try {
                            preparedSender.send();
                            sent.incrementAndGet();
                        } catch (RuntimeException e) {
                            failed.incrementAndGet();
                        }
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "broker-client-forward-qps-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sendersDone.await();
        long endNanos = System.nanoTime();
        long measurementEndNanos = action.measurementEndNanos();
        while (System.nanoTime() < measurementEndNanos) {
            TimeUnit.MILLISECONDS.sleep(1);
        }

        double sendSeconds = Math.max(0.000001, (endNanos - startNanos) / 1_000_000_000.0);
        double receiveSeconds = duration.toNanos() / 1_000_000_000.0;
        return new ThroughputResult(
                AeronTransport.brokerInputPartitionCount(),
                senderThreads,
                sendSeconds,
                receiveSeconds,
                sent.get(),
                failed.get(),
                action.measuredReceived());
    }

    private static void waitUntilReceiverQuiet(ForwardCountingAction action, Duration quietFor, Duration timeout)
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
                "%s: partitions=%d, sendDuration=%.2fs, receiveWindow=%.2fs, senderThreads=%d, sent=%d, failed=%d, received=%d, sendQps=%.2f, forwardQps=%.2f%n",
                label,
                result.partitions(),
                result.sendSeconds(),
                result.receiveSeconds(),
                result.senderThreads(),
                result.sent(),
                result.failed(),
                result.received(),
                result.sendQps(),
                result.forwardQps());
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
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

    @ActionController(CMD)
    private static final class ForwardCountingAction {
        private final AtomicLong totalReceived = new AtomicLong();
        private final AtomicLong measuredReceived = new AtomicLong();
        private final AtomicLong measurementStartNanos = new AtomicLong();
        private final AtomicLong measurementEndNanos = new AtomicLong();

        @ActionMethod(value = ACT, mode = ActionExecutionMode.INLINE)
        public void receive(ActionContext context) {
            recordReceived();
        }

        private void recordReceived() {
            totalReceived.incrementAndGet();
            long now = System.nanoTime();
            long start = measurementStartNanos.get();
            long end = measurementEndNanos.get();
            if (start != 0L && now >= start && now < end) {
                measuredReceived.incrementAndGet();
            }
        }

        private void startMeasurement(Duration duration) {
            measuredReceived.set(0);
            long start = System.nanoTime();
            measurementStartNanos.set(start);
            measurementEndNanos.set(start + duration.toNanos());
        }

        private long totalReceived() {
            return totalReceived.get();
        }

        private long measuredReceived() {
            return measuredReceived.get();
        }

        private long measurementEndNanos() {
            return measurementEndNanos.get();
        }
    }

    private record ThroughputResult(int partitions, int senderThreads, double sendSeconds,
                                    double receiveSeconds, long sent, long failed, long received) {
        private double sendQps() {
            return sendSeconds <= 0 ? 0 : sent / sendSeconds;
        }

        private double forwardQps() {
            return receiveSeconds <= 0 ? 0 : received / receiveSeconds;
        }
    }
}
