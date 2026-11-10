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

class RegistryClusterActiveStandbyFailoverTest {
    private static final int CMD = 111;
    private static final int ACT = 1;
    private static final int DEFAULT_SENDER_THREADS = 4;
    private static final int DEFAULT_MAX_IN_FLIGHT = 4_096;
    private static final int DEFAULT_BEFORE_SECONDS = 3;
    private static final int DEFAULT_SWITCH_DELAY_MILLIS = 1_000;
    private static final int DEFAULT_AFTER_SECONDS = 3;

    @Test
    @Timeout(30)
    void servicesConnectedToBrokerAFailOverToDiscoveredBrokerB() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-active-standby-failover-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
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
        ServiceNode serviceA = null;
        ServiceNode serviceB = null;
        try {
            System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());
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
            System.setProperty("kynet.broker.nodeId", "active-standby-a");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortA));
            brokerA = new RegistryServer(brokerPortA);
            brokerA.start();

            CountingStandbyAction action = new CountingStandbyAction();
            serviceB = new ServiceNode("127.0.0.1", brokerPortA, "ActiveStandbyServiceB");
            serviceB.start();
            new ActionBootstrap(serviceB).registerAction(action);

            serviceA = new ServiceNode("127.0.0.1", brokerPortA, "ActiveStandbyServiceA");
            serviceA.start();

            InvokeOptions options = InvokeOptions.builder()
                    .targetInstanceId(serviceB.getInstanceId())
                    .sessionId(111_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(positiveIntProperty(
                            "kynet.broker.activeStandby.timeoutMs", 1_000)))
                    .build();

            waitForProviderRegistration(serviceA, options, "services on broker A");

            mediaDriverB = launchDriver(brokerPortB);
            System.setProperty("kynet.broker.nodeId", "active-standby-b");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortB));
            brokerB = new RegistryServer(brokerPortB);
            brokerB.start();

            int senderThreads = positiveIntProperty(
                    "kynet.broker.activeStandby.senderThreads", DEFAULT_SENDER_THREADS);
            int maxInFlight = positiveIntProperty(
                    "kynet.broker.activeStandby.maxInFlight", DEFAULT_MAX_IN_FLIGHT);
            int beforeSeconds = positiveIntProperty(
                    "kynet.broker.activeStandby.beforeSeconds", DEFAULT_BEFORE_SECONDS);
            int switchDelayMillis = positiveIntProperty(
                    "kynet.broker.activeStandby.switchDelayMillis", DEFAULT_SWITCH_DELAY_MILLIS);
            int afterSeconds = positiveIntProperty(
                    "kynet.broker.activeStandby.afterSeconds", DEFAULT_AFTER_SECONDS);
            int minimumAfterFailoverQps = positiveIntProperty(
                    "kynet.broker.activeStandby.minimumAfterFailoverQps", 1_000);

            WindowResult before = runRpcWindow(action, serviceA, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(beforeSeconds));
            printWindow("active-standby before brokerA stop", before);

            brokerA.stop();
            brokerA = null;
            TimeUnit.MILLISECONDS.sleep(switchDelayMillis);

            WindowResult after = runRpcWindow(action, serviceA, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(afterSeconds));
            printWindow("active-standby after brokerA stop", after);
            printSummary("active-standby failover result", switchDelayMillis / 1_000.0, before, after);

            assertTrue(before.success() > 0,
                    "service A should complete RPC calls while both services are on broker A");
            assertTrue(after.success() > 0,
                    "service A should complete RPC calls after both services fail over to broker B");
            assertTrue(after.receiverTotalReceived() > 0,
                    "service B should receive RPC calls after reconnecting to broker B");
            assertEquals(0L, after.failed(),
                    "steady-state failover window should not lose RPC calls after the switch delay");
            assertEquals(after.sent(), after.success(),
                    "every RPC sent after the switch delay should complete successfully");
            assertTrue(after.successQps() >= minimumAfterFailoverQps,
                    "failover throughput should satisfy the configured absolute steady-state QPS floor");
        } finally {
            if (serviceA != null) {
                serviceA.shutdown();
            }
            if (serviceB != null) {
                serviceB.shutdown();
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

    private static void waitForProviderRegistration(ServiceNode serviceA, InvokeOptions options, String label)
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
            fail(label + " was not ready before benchmark: " + lastException.getMessage());
        }
        fail(label + " was not ready before benchmark, lastResult=" + describe(lastResult));
    }

    private static WindowResult runRpcWindow(CountingStandbyAction action, ServiceNode serviceA,
                                             InvokeOptions options, int senderThreads, int maxInFlight,
                                             Duration duration) throws InterruptedException {
        RpcCounters counters = new RpcCounters();
        AtomicLong inFlight = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        long receiverTotalBefore = action.totalReceived();
        action.startMeasurement(duration);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastRpcSender preparedSender =
                        serviceA.prepareFastRpcSender(CMD, ACT, null, options);
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        sendOne(preparedSender, counters, inFlight, maxInFlight);
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "active-standby-rpc-sender-" + (i + 1));
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
        double receiveSeconds = duration.toNanos() / 1_000_000_000.0;
        return new WindowResult(
                senderThreads,
                maxInFlight,
                sendSeconds,
                receiveSeconds,
                counters.sent.get(),
                counters.success.get(),
                counters.failed.get(),
                action.measuredReceived(),
                action.totalReceived() - receiverTotalBefore,
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

    private static void printWindow(String label, WindowResult result) {
        System.out.printf(
                "%s: senderThreads=%d, maxInFlight=%d, sendDuration=%.2fs, receiveWindow=%.2fs, sent=%d, success=%d, failed=%d, receiverWindowReceived=%d, receiverTotalReceived=%d, inFlightLeft=%d, sendQps=%.2f, successQps=%.2f, receiverQps=%.2f%n",
                label,
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
                                     WindowResult before, WindowResult after) {
        System.out.printf(
                "%s: switchDelay=%.2fs, beforeBrokerAStop=[sent=%d, success=%d, failed=%d, receiverReceived=%d, successQps=%.2f, receiverQps=%.2f], afterBrokerAStop=[sent=%d, success=%d, failed=%d, receiverReceived=%d, successQps=%.2f, receiverQps=%.2f]%n",
                label,
                switchDelaySeconds,
                before.sent(),
                before.success(),
                before.failed(),
                before.receiverWindowReceived(),
                before.successQps(),
                before.receiverQps(),
                after.sent(),
                after.success(),
                after.failed(),
                after.receiverWindowReceived(),
                after.successQps(),
                after.receiverQps());
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
    private static final class CountingStandbyAction {
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

    private record WindowResult(int senderThreads, int maxInFlight,
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
