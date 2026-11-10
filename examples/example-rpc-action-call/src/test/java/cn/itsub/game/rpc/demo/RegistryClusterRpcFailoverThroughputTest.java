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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RegistryClusterRpcFailoverThroughputTest {
    private static final int CMD = 107;
    private static final int ACT = 1;
    private static final int DEFAULT_SENDER_THREADS = 4;
    private static final int DEFAULT_MAX_IN_FLIGHT = 512;
    private static final int DEFAULT_WARMUP_SECONDS = 5;
    private static final int DEFAULT_DURATION_SECONDS = 20;
    private static final int DEFAULT_FAILOVER_AT_SECONDS = 10;
    private static final int DEFAULT_RECOVERED_STEADY_SECONDS = 3;
    private static final int DEFAULT_MINIMUM_RECOVERED_QPS = 1_000;
    private static final int DEFAULT_MINIMUM_CROSS_BROKER_QPS = 1_000;

    @Test
    @Timeout(45)
    void measuresRpcThroughputBeforeAndAfterProviderBrokerFailsOver() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-cluster-rpc-failover-" + UUID.randomUUID()).toAbsolutePath();
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
        ServiceNode provider = null;
        ServiceNode caller = null;
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
            System.setProperty("kynet.broker.nodeId", "cluster-rpc-failover-a");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortA));
            brokerA = new RegistryServer(brokerPortA);
            brokerA.start();

            mediaDriverB = launchDriver(brokerPortB);
            System.setProperty("kynet.broker.nodeId", "cluster-rpc-failover-b");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortB));
            brokerB = new RegistryServer(brokerPortB);
            brokerB.start();

            provider = new ServiceNode("127.0.0.1", brokerPortA, "ClusterRpcFailoverProvider");
            provider.start();
            new ActionBootstrap(provider).registerAction(new InlineRpcAction());

            caller = new ServiceNode("127.0.0.1", brokerPortB, "ClusterRpcFailoverCaller");
            caller.start();

            InvokeOptions options = InvokeOptions.builder()
                    .sessionId(107_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(positiveIntProperty(
                            "kynet.broker.rpcFailover.timeoutMs", 1_000)))
                    .build();

            waitForProviderRegistration(caller, options);

            int senderThreads = positiveIntProperty(
                    "kynet.broker.rpcFailover.senderThreads", DEFAULT_SENDER_THREADS);
            int maxInFlight = positiveIntProperty(
                    "kynet.broker.rpcFailover.maxInFlight", DEFAULT_MAX_IN_FLIGHT);
            int warmupSeconds = positiveIntProperty(
                    "kynet.broker.rpcFailover.warmupSeconds", DEFAULT_WARMUP_SECONDS);
            int durationSeconds = positiveIntProperty(
                    "kynet.broker.rpcFailover.durationSeconds", DEFAULT_DURATION_SECONDS);
            int failoverAtSeconds = positiveIntProperty(
                    "kynet.broker.rpcFailover.failoverAtSeconds", DEFAULT_FAILOVER_AT_SECONDS);
            int recoveredSteadySeconds = positiveIntProperty(
                    "kynet.broker.rpcFailover.recoveredSteadySeconds", DEFAULT_RECOVERED_STEADY_SECONDS);
            int minimumRecoveredQps = positiveIntProperty(
                    "kynet.broker.rpcFailover.minimumRecoveredQps", DEFAULT_MINIMUM_RECOVERED_QPS);
            int minimumCrossBrokerQps = positiveIntProperty(
                    "kynet.broker.rpcFailover.minimumCrossBrokerQps", DEFAULT_MINIMUM_CROSS_BROKER_QPS);

            PhaseResult warmup = runWarmup(caller, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(warmupSeconds));
            printWarmup("broker rpc failover warmup", warmup);

            AtomicReference<RegistryServer> brokerARef = new AtomicReference<>(brokerA);
            FailoverResult measured = runFailoverWindow(
                    caller,
                    options,
                    senderThreads,
                    maxInFlight,
                    Duration.ofSeconds(durationSeconds),
                    Duration.ofSeconds(failoverAtSeconds),
                    () -> {
                        RegistryServer stopped = brokerARef.getAndSet(null);
                        if (stopped != null) {
                            stopped.stop();
                        }
                    });
            brokerA = brokerARef.get();

            printResult("broker rpc failover throughput", measured);
            waitForProviderRegistration(caller, options);
            PhaseResult recoveredSteady = runWarmup(caller, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(recoveredSteadySeconds));
            printWarmup("broker rpc recovered steady state", recoveredSteady);
            assertTrue(measured.before().success() > 0,
                    "first half should complete cross-broker RPC calls before broker A stops");
            assertTrue(measured.before().successQps() >= minimumCrossBrokerQps,
                    "cross-broker throughput should satisfy the configured steady-state QPS floor");
            assertTrue(measured.after().success() > 0,
                    "second half should complete RPC calls after provider reconnects to broker B");
            assertEquals(0L, recoveredSteady.failed(),
                    "recovered steady-state window should not lose RPC calls");
            assertEquals(recoveredSteady.sent(), recoveredSteady.success(),
                    "every RPC sent after recovery should complete successfully");
            assertTrue(recoveredSteady.successQps() >= minimumRecoveredQps,
                    "recovered steady-state throughput should satisfy the configured QPS floor");
            assertEquals(0L, measured.inFlightLeft(),
                    "continuous failover window should drain every in-flight RPC");
        } finally {
            if (caller != null) {
                caller.shutdown();
            }
            if (provider != null) {
                provider.shutdown();
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

    private static void waitForProviderRegistration(ServiceNode caller, InvokeOptions options)
            throws InterruptedException {
        InvokeOptions probeOptions = options.toBuilder()
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(8).toNanos();
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
            TimeUnit.MILLISECONDS.sleep(50);
        }
        if (lastException != null) {
            fail("provider action 107:1 was not ready before benchmark: " + lastException.getMessage());
        }
        fail("provider action 107:1 was not ready before benchmark, lastResult=" + describe(lastResult));
    }

    private static PhaseResult runWarmup(ServiceNode caller, InvokeOptions options, int senderThreads,
                                         int maxInFlight, Duration duration) throws InterruptedException {
        PhaseCounters counters = new PhaseCounters();
        AtomicLong inFlight = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                try {
                    sendUntil(caller, options, counters, inFlight, maxInFlight, deadlineNanos);
                } finally {
                    sendersDone.countDown();
                }
            }, "broker-rpc-failover-warmup-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }
        waitForSenders(sendersDone, senderThreads);
        drainInFlight(inFlight, options.getTimeout());
        return counters.snapshot(duration.toNanos() / 1_000_000_000.0);
    }

    private static FailoverResult runFailoverWindow(ServiceNode caller, InvokeOptions options, int senderThreads,
                                                    int maxInFlight, Duration duration, Duration failoverAfter,
                                                    Runnable failover) throws InterruptedException {
        PhaseCounters before = new PhaseCounters();
        PhaseCounters after = new PhaseCounters();
        AtomicLong inFlight = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);
        long startNanos = System.nanoTime();
        long failoverNanos = startNanos + failoverAfter.toNanos();
        long deadlineNanos = startNanos + duration.toNanos();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastRpcSender preparedSender =
                        caller.prepareFastRpcSender(CMD, ACT, null, options);
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        PhaseCounters counters = System.nanoTime() < failoverNanos ? before : after;
                        sendOne(preparedSender, counters, inFlight, maxInFlight);
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "broker-rpc-failover-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sleepUntil(failoverNanos);
        failover.run();

        sendersDone.await();
        drainInFlight(inFlight, options.getTimeout());
        double beforeSeconds = failoverAfter.toNanos() / 1_000_000_000.0;
        double afterSeconds = Math.max(0.000001,
                (duration.toNanos() - failoverAfter.toNanos()) / 1_000_000_000.0);
        return new FailoverResult(
                senderThreads,
                maxInFlight,
                before.snapshot(beforeSeconds),
                after.snapshot(afterSeconds),
                inFlight.get());
    }

    private static void sendUntil(ServiceNode caller, InvokeOptions options, PhaseCounters counters,
                                  AtomicLong inFlight, int maxInFlight, long deadlineNanos) {
        ServiceNode.FastRpcSender preparedSender =
                caller.prepareFastRpcSender(CMD, ACT, null, options);
        while (System.nanoTime() < deadlineNanos) {
            sendOne(preparedSender, counters, inFlight, maxInFlight);
        }
    }

    private static void sendOne(ServiceNode.FastRpcSender sender, PhaseCounters counters,
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

    private static void sleepUntil(long deadlineNanos) throws InterruptedException {
        while (true) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return;
            }
            TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(10), remainingNanos));
        }
    }

    private static void printWarmup(String label, PhaseResult result) {
        System.out.printf(
                "%s: duration=%.2fs, sent=%d, success=%d, failed=%d, sendQps=%.2f, successQps=%.2f%n",
                label,
                result.seconds(),
                result.sent(),
                result.success(),
                result.failed(),
                result.sendQps(),
                result.successQps());
    }

    private static void printResult(String label, FailoverResult result) {
        System.out.printf(
                "%s: senderThreads=%d, maxInFlight=%d, crossBroker=[duration=%.2fs, sent=%d, success=%d, failed=%d, sendQps=%.2f, successQps=%.2f], afterBrokerAStop=[duration=%.2fs, sent=%d, success=%d, failed=%d, sendQps=%.2f, successQps=%.2f], inFlightLeft=%d%n",
                label,
                result.senderThreads(),
                result.maxInFlight(),
                result.before().seconds(),
                result.before().sent(),
                result.before().success(),
                result.before().failed(),
                result.before().sendQps(),
                result.before().successQps(),
                result.after().seconds(),
                result.after().sent(),
                result.after().success(),
                result.after().failed(),
                result.after().sendQps(),
                result.after().successQps(),
                result.inFlightLeft());
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
    private static final class InlineRpcAction {
        @ActionMethod(act = ACT, mode = ActionExecutionMode.INLINE)
        public ActionResult invoke(ActionContext context) {
            return ActionResult.success(null);
        }
    }

    private static final class PhaseCounters {
        private final AtomicLong sent = new AtomicLong();
        private final AtomicLong success = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();

        private PhaseResult snapshot(double seconds) {
            return new PhaseResult(seconds, sent.get(), success.get(), failed.get());
        }
    }

    private record PhaseResult(double seconds, long sent, long success, long failed) {
        private double sendQps() {
            return seconds <= 0 ? 0 : sent / seconds;
        }

        private double successQps() {
            return seconds <= 0 ? 0 : success / seconds;
        }
    }

    private record FailoverResult(int senderThreads, int maxInFlight,
                                  PhaseResult before, PhaseResult after, long inFlightLeft) {
    }
}
