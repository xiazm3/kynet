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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RegistryClusterForwardFailoverReceiverQpsTest {
    private static final int CMD = 108;
    private static final int ACT = 1;
    private static final int DEFAULT_BROKER_PARTITIONS = 2;
    private static final int DEFAULT_SENDER_THREADS = 4;
    private static final int DEFAULT_MAX_IN_FLIGHT = 512;
    private static final int DEFAULT_WARMUP_SECONDS = 5;
    private static final int DEFAULT_CROSS_BROKER_SECONDS = 10;
    private static final int DEFAULT_SWITCH_DELAY_MILLIS = 1_000;
    private static final int DEFAULT_LOCAL_BROKER_SECONDS = 10;
    private static final int DEFAULT_MINIMUM_STEADY_QPS = 1_000;

    @Test
    @Timeout(420)
    void measuresCrossBrokerAndSameBrokerForwardingAfterFailoverAtReceiver() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-cluster-forward-failover-qps-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        String previousBrokerPartitions = System.getProperty("kynet.broker.partitions");
        String previousClusterEnabled = System.getProperty("kynet.broker.cluster.enabled");
        String previousClusterSecret = System.getProperty("kynet.broker.cluster.secret");
        String previousClusterHost = System.getProperty("kynet.broker.clusterHost");
        String previousClusterBindHost = System.getProperty("kynet.broker.clusterBindHost");
        String previousClientHost = System.getProperty("kynet.broker.clientHost");
        String previousDiscoveryAddress = System.getProperty("kynet.broker.discoveryAddress");
        String previousDiscoveryPort = System.getProperty("kynet.broker.discoveryPort");
        String previousDiscoveryInterval = System.getProperty("kynet.broker.discoveryIntervalMs");
        String previousGossipInterval = System.getProperty("kynet.broker.gossipIntervalMs");
        String previousSuspectTimeout = System.getProperty("kynet.broker.suspectTimeoutMs");
        String previousDeadTimeout = System.getProperty("kynet.broker.deadTimeoutMs");
        String previousNodeId = System.getProperty("kynet.broker.nodeId");
        String previousClusterPort = System.getProperty("kynet.broker.clusterPort");

        int brokerPortA = AeronTransport.findAvailablePort();
        int brokerPortB = AeronTransport.findAvailablePort();
        int clusterPortA = AeronTransport.findAvailablePort();
        int clusterPortB = AeronTransport.findAvailablePort();
        int discoveryPort = AeronTransport.findAvailablePort();

        MediaDriver mediaDriverA = null;
        MediaDriver mediaDriverB = null;
        RegistryServer brokerA = null;
        RegistryServer brokerB = null;
        ServiceNode receiver = null;
        ServiceNode sender = null;
        try {
            System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());
            System.setProperty("kynet.broker.partitions", String.valueOf(
                    positiveIntProperty("kynet.broker.forwardFailover.partitions", DEFAULT_BROKER_PARTITIONS)));
            System.setProperty("kynet.broker.cluster.enabled", "true");
            System.setProperty("kynet.broker.cluster.secret", "kynet-cluster-test-secret");
            System.setProperty("kynet.broker.clusterHost", "127.0.0.1");
            System.setProperty("kynet.broker.clusterBindHost", "127.0.0.1");
            System.setProperty("kynet.broker.clientHost", "127.0.0.1");
            System.setProperty("kynet.broker.discoveryAddress", "255.255.255.255");
            System.setProperty("kynet.broker.discoveryPort", String.valueOf(discoveryPort));
            System.setProperty("kynet.broker.discoveryIntervalMs", "100");
            System.setProperty("kynet.broker.gossipIntervalMs", "100");
            System.setProperty("kynet.broker.suspectTimeoutMs", "500");
            System.setProperty("kynet.broker.deadTimeoutMs", "1000");

            mediaDriverA = launchDriver(brokerPortA);
            System.setProperty("kynet.broker.nodeId", "cluster-forward-failover-a");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortA));
            brokerA = new RegistryServer(brokerPortA);
            brokerA.start();

            mediaDriverB = launchDriver(brokerPortB);
            System.setProperty("kynet.broker.nodeId", "cluster-forward-failover-b");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortB));
            brokerB = new RegistryServer(brokerPortB);
            brokerB.start();

            CountingRpcAction receiverAction = new CountingRpcAction();
            receiver = new ServiceNode("127.0.0.1", brokerPortA, "ClusterForwardFailoverReceiver");
            receiver.start();
            new ActionBootstrap(receiver).registerAction(receiverAction);

            sender = new ServiceNode("127.0.0.1", brokerPortB, "ClusterForwardFailoverSender");
            sender.start();

            InvokeOptions options = InvokeOptions.builder()
                    .targetInstanceId(receiver.getInstanceId())
                    .sessionId(108_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(positiveIntProperty(
                            "kynet.broker.forwardFailover.timeoutMs", 1_000)))
                    .build();

            int senderThreads = positiveIntProperty(
                    "kynet.broker.forwardFailover.senderThreads", DEFAULT_SENDER_THREADS);
            int maxInFlight = positiveIntProperty(
                    "kynet.broker.forwardFailover.maxInFlight", DEFAULT_MAX_IN_FLIGHT);
            int warmupSeconds = positiveIntProperty(
                    "kynet.broker.forwardFailover.warmupSeconds", DEFAULT_WARMUP_SECONDS);
            int crossBrokerSeconds = positiveIntProperty(
                    "kynet.broker.forwardFailover.crossBrokerSeconds", DEFAULT_CROSS_BROKER_SECONDS);
            int switchDelayMillis = positiveIntProperty(
                    "kynet.broker.forwardFailover.switchDelayMillis", DEFAULT_SWITCH_DELAY_MILLIS);
            int localBrokerSeconds = positiveIntProperty(
                    "kynet.broker.forwardFailover.localBrokerSeconds", DEFAULT_LOCAL_BROKER_SECONDS);
            int minimumSteadyQps = positiveIntProperty(
                    "kynet.broker.forwardFailover.minimumSteadyQps", DEFAULT_MINIMUM_STEADY_QPS);

            waitForProviderRegistration(sender, options, Duration.ofSeconds(8), "cross-broker route");
            waitUntilReceiverQuiet(receiverAction, Duration.ofMillis(200), Duration.ofSeconds(3));

            WindowResult warmup = runRpcWindow(receiverAction, sender, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(warmupSeconds));
            printWindow("broker cluster forward warmup", warmup);
            waitUntilReceiverQuiet(receiverAction, Duration.ofMillis(200), Duration.ofSeconds(3));

            WindowResult crossBroker = runRpcWindow(receiverAction, sender, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(crossBrokerSeconds));
            printWindow("broker cluster cross-broker forward throughput", crossBroker);

            brokerA.stop();
            brokerA = null;
            TimeUnit.MILLISECONDS.sleep(switchDelayMillis);

            WindowResult sameBroker = runRpcWindow(receiverAction, sender, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(localBrokerSeconds));
            printWindow("broker cluster same-broker forward after brokerA stop", sameBroker);

            printSummary("broker cluster forward failover receiver qps",
                    switchDelayMillis / 1_000.0,
                    crossBroker,
                    sameBroker);

            assertTrue(crossBroker.receiverWindowReceived() > 0,
                    "receiver should observe cross-broker forwarded RPC calls");
            assertTrue(crossBroker.success() > 0,
                    "sender should observe cross-broker RPC successes");
            assertTrue(sameBroker.receiverWindowReceived() > 0,
                    "receiver should observe same-broker RPC calls after broker A stops");
            assertTrue(sameBroker.success() > 0,
                    "sender should observe RPC successes after receiver reconnects to broker B");
            assertEquals(0L, crossBroker.failed(),
                    "steady cross-broker window should not lose RPC calls");
            assertEquals(crossBroker.sent(), crossBroker.success(),
                    "every cross-broker RPC should complete successfully");
            assertEquals(0L, sameBroker.failed(),
                    "steady post-failover window should not lose RPC calls");
            assertEquals(sameBroker.sent(), sameBroker.success(),
                    "every RPC sent after receiver recovery should complete successfully");
            assertEquals(0L, sameBroker.inFlightLeft(),
                    "post-failover window should drain every in-flight RPC");
            assertTrue(crossBroker.successQps() >= minimumSteadyQps,
                    "cross-broker throughput should satisfy the configured steady-state QPS floor");
            assertTrue(sameBroker.successQps() >= minimumSteadyQps,
                    "post-failover throughput should satisfy the configured steady-state QPS floor");
        } finally {
            if (sender != null) {
                sender.shutdown();
            }
            if (receiver != null) {
                receiver.shutdown();
            }
            if (brokerA != null) {
                brokerA.stop();
            }
            if (brokerB != null) {
                brokerB.stop();
            }
            if (mediaDriverA != null) {
                mediaDriverA.close();
            }
            if (mediaDriverB != null) {
                mediaDriverB.close();
            }
            restoreProperty("kynet.broker.clusterPort", previousClusterPort);
            restoreProperty("kynet.broker.nodeId", previousNodeId);
            restoreProperty("kynet.broker.deadTimeoutMs", previousDeadTimeout);
            restoreProperty("kynet.broker.suspectTimeoutMs", previousSuspectTimeout);
            restoreProperty("kynet.broker.gossipIntervalMs", previousGossipInterval);
            restoreProperty("kynet.broker.discoveryIntervalMs", previousDiscoveryInterval);
            restoreProperty("kynet.broker.discoveryPort", previousDiscoveryPort);
            restoreProperty("kynet.broker.discoveryAddress", previousDiscoveryAddress);
            restoreProperty("kynet.broker.clientHost", previousClientHost);
            restoreProperty("kynet.broker.clusterBindHost", previousClusterBindHost);
            restoreProperty("kynet.broker.clusterHost", previousClusterHost);
            restoreProperty("kynet.broker.cluster.enabled", previousClusterEnabled);
            restoreProperty("kynet.broker.cluster.secret", previousClusterSecret);
            restoreProperty("kynet.broker.partitions", previousBrokerPartitions);
            restoreProperty("kynet.aeron.baseDir", previousBaseDir);
        }
    }

    private static MediaDriver launchDriver(int brokerPort) {
        return MediaDriver.launchEmbedded(new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.DEDICATED)
                .aeronDirectoryName(AeronTransport.brokerAeronDirectory(brokerPort)));
    }

    private static void waitForProviderRegistration(ServiceNode sender, InvokeOptions options,
                                                    Duration timeout, String label)
            throws InterruptedException {
        InvokeOptions probeOptions = options.toBuilder()
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
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
            TimeUnit.MILLISECONDS.sleep(50);
        }
        if (lastException != null) {
            fail(label + " was not ready before benchmark: " + lastException.getMessage());
        }
        fail(label + " was not ready before benchmark, lastResult=" + describe(lastResult));
    }

    private static WindowResult runRpcWindow(CountingRpcAction receiverAction, ServiceNode sender,
                                             InvokeOptions options, int senderThreads, int maxInFlight,
                                             Duration duration) throws InterruptedException {
        RpcCounters counters = new RpcCounters();
        AtomicLong inFlight = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        long receiverTotalBefore = receiverAction.totalReceived();
        receiverAction.startMeasurement(duration);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastRpcSender preparedSender =
                        sender.prepareFastRpcSender(CMD, ACT, null, options);
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        sendOne(preparedSender, counters, inFlight, maxInFlight);
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "broker-cluster-forward-failover-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        waitForSenders(sendersDone, senderThreads);
        long endNanos = System.nanoTime();
        drainInFlight(inFlight, options.getTimeout());
        while (System.nanoTime() < receiverAction.measurementEndNanos()) {
            TimeUnit.MILLISECONDS.sleep(1);
        }

        double sendSeconds = Math.max(0.000001, (endNanos - startNanos) / 1_000_000_000.0);
        double receiveSeconds = duration.toNanos() / 1_000_000_000.0;
        long receiverTotalDelta = receiverAction.totalReceived() - receiverTotalBefore;
        return new WindowResult(
                AeronTransport.brokerInputPartitionCount(),
                senderThreads,
                maxInFlight,
                sendSeconds,
                receiveSeconds,
                counters.sent.get(),
                counters.success.get(),
                counters.failed.get(),
                receiverAction.measuredReceived(),
                receiverTotalDelta,
                inFlight.get());
    }

    private static void sendOne(ServiceNode.FastRpcSender sender, RpcCounters counters,
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
                inFlight.decrementAndGet();
                if (throwable != null || result == null || !result.isSuccess()) {
                    counters.failed.incrementAndGet();
                    return;
                }
                counters.success.incrementAndGet();
            });
        } catch (RuntimeException e) {
            inFlight.decrementAndGet();
            counters.failed.incrementAndGet();
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

    private static void waitUntilReceiverQuiet(CountingRpcAction action, Duration quietFor, Duration timeout)
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

    private static void printWindow(String label, WindowResult result) {
        System.out.printf(
                "%s: partitions=%d, senderThreads=%d, maxInFlight=%d, sendDuration=%.2fs, receiveWindow=%.2fs, sent=%d, success=%d, failed=%d, receiverWindowReceived=%d, receiverTotalReceived=%d, inFlightLeft=%d, sendQps=%.2f, successQps=%.2f, receiverQps=%.2f%n",
                label,
                result.partitions(),
                result.senderThreads(),
                result.maxInFlight(),
                result.sendSeconds(),
                result.receiveSeconds(),
                result.sent(),
                result.success(),
                result.failed(),
                result.receiverWindowReceived(),
                result.receiverTotalReceived(),
                result.inFlightLeft(),
                result.sendQps(),
                result.successQps(),
                result.receiverQps());
    }

    private static void printSummary(String label, double switchDelaySeconds,
                                     WindowResult crossBroker, WindowResult sameBroker) {
        System.out.printf(
                "%s: switchDelay=%.2fs, crossBroker=[sent=%d, success=%d, failed=%d, receiverReceived=%d, successQps=%.2f, receiverQps=%.2f], sameBrokerAfterBrokerAStop=[sent=%d, success=%d, failed=%d, receiverReceived=%d, successQps=%.2f, receiverQps=%.2f]%n",
                label,
                switchDelaySeconds,
                crossBroker.sent(),
                crossBroker.success(),
                crossBroker.failed(),
                crossBroker.receiverWindowReceived(),
                crossBroker.successQps(),
                crossBroker.receiverQps(),
                sameBroker.sent(),
                sameBroker.success(),
                sameBroker.failed(),
                sameBroker.receiverWindowReceived(),
                sameBroker.successQps(),
                sameBroker.receiverQps());
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

    @ActionController(cmd = CMD)
    private static final class CountingRpcAction {
        private final AtomicLong totalReceived = new AtomicLong();
        private final AtomicLong measuredReceived = new AtomicLong();
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

    private static final class RpcCounters {
        private final AtomicLong sent = new AtomicLong();
        private final AtomicLong success = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();
    }

    private record WindowResult(int partitions, int senderThreads, int maxInFlight,
                                double sendSeconds, double receiveSeconds,
                                long sent, long success, long failed,
                                long receiverWindowReceived, long receiverTotalReceived,
                                long inFlightLeft) {
        private double sendQps() {
            return sendSeconds <= 0 ? 0 : sent / sendSeconds;
        }

        private double successQps() {
            return sendSeconds <= 0 ? 0 : success / sendSeconds;
        }

        private double receiverQps() {
            return receiveSeconds <= 0 ? 0 : receiverWindowReceived / receiveSeconds;
        }
    }
}
