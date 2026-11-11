package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.registry.RegistryServer;
import cn.itsub.kynet.common.transport.AeronTransport;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.action.ActionContext;
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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RegistryClusterHighAvailabilityTest {
    private static final int CMD = 106;
    private static final int ACT = 1;
    private static final int LARGE_ACT = 2;

    @Test
    @Timeout(30)
    void idempotentInFlightRpcSurvivesProviderBrokerFailoverWithoutDuplicateExecution() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-cluster-ha-" + UUID.randomUUID()).toAbsolutePath();
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

        Set<Integer> allocatedPorts = new HashSet<>();
        int brokerPortA = uniqueAvailablePort(allocatedPorts);
        int brokerPortB = uniqueAvailablePort(allocatedPorts);
        int brokerPortC = uniqueAvailablePort(allocatedPorts);
        int clusterPortA = uniqueAvailablePort(allocatedPorts);
        int clusterPortB = uniqueAvailablePort(allocatedPorts);
        int clusterPortC = uniqueAvailablePort(allocatedPorts);
        int discoveryPort = uniqueAvailablePort(allocatedPorts);

        MediaDriver mediaDriverA = null;
        MediaDriver mediaDriverB = null;
        MediaDriver mediaDriverC = null;
        RegistryServer brokerA = null;
        RegistryServer brokerB = null;
        RegistryServer brokerC = null;
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
            System.setProperty("kynet.broker.nodeId", "cluster-ha-a");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortA));
            brokerA = new RegistryServer(brokerPortA);
            brokerA.start();

            mediaDriverB = launchDriver(brokerPortB);
            System.setProperty("kynet.broker.nodeId", "cluster-ha-b");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortB));
            brokerB = new RegistryServer(brokerPortB);
            brokerB.start();

            mediaDriverC = launchDriver(brokerPortC);
            System.setProperty("kynet.broker.nodeId", "cluster-ha-c");
            System.setProperty("kynet.broker.clusterPort", String.valueOf(clusterPortC));
            brokerC = new RegistryServer(brokerPortC);
            brokerC.start();

            ClusterEchoAction action = new ClusterEchoAction();
            provider = new ServiceNode("127.0.0.1", brokerPortA, "ClusterHaProvider");
            provider.start();
            new ActionBootstrap(provider).registerAction(action);

            caller = new ServiceNode("127.0.0.1", brokerPortB, "ClusterHaCaller");
            caller.start();

            InvokeOptions options = InvokeOptions.builder()
                    .sessionId(106_001L)
                    .userId(1L)
                    .timeout(Duration.ofSeconds(5))
                    .idempotent(true)
                    .maxRetries(30)
                    .retryBackoff(Duration.ofMillis(100))
                    .build();

            waitForSuccess(caller, options, "cross-broker route");
            RegistryServer survivingBrokerB = brokerB;
            RegistryServer survivingBrokerC = brokerC;
            waitForCondition(() -> survivingBrokerB.clusterMetrics().aliveMembers() == 3
                            && survivingBrokerC.clusterMetrics().aliveMembers() == 3,
                    Duration.ofSeconds(5), "three brokers should converge before failure injection");

            byte[] largePayload = new byte[900 * 1024];
            for (int index = 0; index < largePayload.length; index++) {
                largePayload[index] = (byte) (index * 31);
            }
            ActionResult largeResult = caller.invoke(CMD, LARGE_ACT, largePayload, options);
            assertTrue(largeResult.isSuccess(),
                    "large cross-broker RPC should succeed: " + describe(largeResult));
            assertArrayEquals(largePayload, (byte[]) largeResult.getData());
            assertEquals(0, brokerA.registryMetrics().rejectedMessages());
            assertEquals(0, brokerB.registryMetrics().rejectedMessages());
            assertTrue(caller.directSentCount() > 0 && provider.directSentCount() > 0);

            int executionsBeforeFailover = action.executions.get();
            action.blockNextRequest();
            CompletableFuture<ActionResult> inFlight = caller.invokeAsync(CMD, ACT, null, options);
            assertTrue(action.entered.await(3, TimeUnit.SECONDS),
                    "provider should start the in-flight RPC before broker shutdown");

            brokerA.stopImmediately();
            brokerA = null;
            action.release.countDown();

            ActionResult recovered = inFlight.get(8, TimeUnit.SECONDS);
            assertTrue(recovered.isSuccess(), "idempotent in-flight RPC should recover after failover: "
                    + describe(recovered));
            assertEquals(executionsBeforeFailover + 1, action.executions.get(),
                    "retried idempotent RPC must not execute the action twice");

            waitForSuccess(caller, options, "provider reconnect after broker failure");
            waitForCondition(() -> survivingBrokerB.clusterMetrics().unavailableMembers() >= 1
                            && survivingBrokerC.clusterMetrics().unavailableMembers() >= 1,
                    Duration.ofSeconds(5),
                    "surviving brokers should detect an unannounced broker loss");
            assertEquals(0, survivingBrokerB.registryMetrics().rejectedMessages(),
                    "surviving registry should accept only control messages");
            assertEquals(0, survivingBrokerC.registryMetrics().rejectedMessages(),
                    "third registry should accept only control messages");
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
            if (brokerC != null) {
                brokerC.stop();
            }
            if (mediaDriverA != null) {
                mediaDriverA.close();
            }
            if (mediaDriverB != null) {
                mediaDriverB.close();
            }
            if (mediaDriverC != null) {
                mediaDriverC.close();
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

    private static int uniqueAvailablePort(Set<Integer> allocatedPorts) {
        int port;
        do {
            port = AeronTransport.findAvailablePort();
        } while (!allocatedPorts.add(port));
        return port;
    }

    private static void waitForSuccess(ServiceNode caller, InvokeOptions options, String label)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        ActionResult lastResult = null;
        RuntimeException lastException = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                lastResult = caller.invoke(CMD, ACT, null, options);
                if (lastResult.isSuccess()) {
                    assertTrue("cluster-ok".equals(lastResult.getData())
                                    || (lastResult.getData() instanceof byte[]),
                            "unexpected cluster action result");
                    return;
                }
            } catch (RuntimeException e) {
                lastException = e;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        if (lastException != null) {
            fail(label + " did not become ready: " + lastException.getMessage());
        }
        fail(label + " did not become ready, lastResult=" + describe(lastResult));
    }

    private static void waitForCondition(BooleanSupplier condition, Duration timeout, String failureMessage)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        fail(failureMessage);
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
    private static final class ClusterEchoAction {
        private final AtomicInteger executions = new AtomicInteger();
        private final AtomicBoolean blockNext = new AtomicBoolean();
        private volatile CountDownLatch entered = new CountDownLatch(0);
        private volatile CountDownLatch release = new CountDownLatch(0);

        private void blockNextRequest() {
            entered = new CountDownLatch(1);
            release = new CountDownLatch(1);
            blockNext.set(true);
        }

        @ActionMethod(ACT)
        public ActionResult echo(ActionContext context) throws InterruptedException {
            executions.incrementAndGet();
            if (blockNext.compareAndSet(true, false)) {
                entered.countDown();
                release.await();
            }
            return ActionResult.success("cluster-ok");
        }

        @ActionMethod(LARGE_ACT)
        public byte[] echoLarge(byte[] payload, ActionContext context) {
            return payload;
        }
    }
}
