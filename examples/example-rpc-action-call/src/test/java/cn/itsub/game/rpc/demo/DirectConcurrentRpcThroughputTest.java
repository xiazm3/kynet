package cn.itsub.game.rpc.demo;

import cn.itsub.game.rpc.demo.action.RpcThroughputAction;
import cn.itsub.kynet.registry.RegistryServer;
import cn.itsub.kynet.common.transport.AeronTransport;
import cn.itsub.kynet.framework.action.ActionBootstrap;
import cn.itsub.kynet.framework.action.ActionResult;
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

class DirectConcurrentRpcThroughputTest {
    private static final int CMD = 99;
    private static final int ACT = 1;
    private static final int DEFAULT_BROKER_PARTITIONS = 2;
    private static final int DEFAULT_SENDER_THREADS = 8;
    private static final int DEFAULT_MAX_IN_FLIGHT = 16_384;
    private static final int DEFAULT_WARMUP_SECONDS = 5;
    private static final int DEFAULT_DURATION_SECONDS = 10;

    @Test
    @Timeout(45)
    void measuresConcurrentRpcRequestsPerSecondBetweenServices() throws Exception {
        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-aeron-benchmark-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);

        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        String previousBrokerPartitions = System.getProperty("kynet.broker.partitions");
        System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());
        if (previousBrokerPartitions == null || previousBrokerPartitions.isBlank()) {
            System.setProperty("kynet.broker.partitions", String.valueOf(
                    positiveIntProperty("kynet.broker.benchmark.partitions", DEFAULT_BROKER_PARTITIONS)));
        }

        int port = AeronTransport.findAvailablePort();
        String brokerDirectory = AeronTransport.brokerAeronDirectory(port);
        MediaDriver mediaDriver = null;
        RegistryServer brokerServer = null;
        ServiceNode provider = null;
        ServiceNode caller = null;
        try {
            mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .threadingMode(ThreadingMode.DEDICATED)
                    .aeronDirectoryName(brokerDirectory));

            brokerServer = new RegistryServer(port);
            brokerServer.start();

            provider = new ServiceNode("127.0.0.1", port, "ThroughputProvider");
            provider.start();
            new ActionBootstrap(provider).registerAction(new RpcThroughputAction());

            caller = new ServiceNode("127.0.0.1", port, "ThroughputCaller");
            caller.start();

            InvokeOptions options = InvokeOptions.builder()
                    .sessionId(99_001L)
                    .userId(1L)
                    .timeout(Duration.ofMillis(positiveIntProperty("kynet.broker.benchmark.timeoutMs", 3_000)))
                    .build();

            waitForProviderRegistration(caller);

            int senderThreads = positiveIntProperty("kynet.broker.benchmark.senderThreads", DEFAULT_SENDER_THREADS);
            int maxInFlight = positiveIntProperty("kynet.broker.benchmark.maxInFlight", DEFAULT_MAX_IN_FLIGHT);
            int warmupSeconds = nonNegativeIntProperty("kynet.broker.benchmark.warmupSeconds", DEFAULT_WARMUP_SECONDS);
            int durationSeconds = positiveIntProperty("kynet.broker.benchmark.durationSeconds", DEFAULT_DURATION_SECONDS);
            boolean fastSender = Boolean.getBoolean("kynet.broker.benchmark.fastSender");
            boolean fastCallback = Boolean.getBoolean("kynet.broker.benchmark.fastCallback");

            if (warmupSeconds > 0) {
                ThroughputResult warmup = runWindow(caller, options, senderThreads, maxInFlight,
                        Duration.ofSeconds(warmupSeconds), fastSender, fastCallback);
                printResult("broker rpc warmup", warmup);
            }

            ThroughputResult measured = runWindow(caller, options, senderThreads, maxInFlight,
                    Duration.ofSeconds(durationSeconds), fastSender, fastCallback);
            printResult("broker rpc throughput", measured);

            assertTrue(measured.success() > 0,
                    "broker should complete at least one RPC request during the measured window");
        } finally {
            if (caller != null) {
                caller.shutdown();
            }
            if (provider != null) {
                provider.shutdown();
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

    private static void waitForProviderRegistration(ServiceNode caller) throws InterruptedException {
        InvokeOptions probeOptions = InvokeOptions.builder()
                .timeout(Duration.ofMillis(500))
                .build();
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
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
            Thread.sleep(25);
        }
        if (lastException != null) {
            fail("provider action 99:1 was not ready before benchmark: " + lastException.getMessage());
        }
        fail("provider action 99:1 was not ready before benchmark, lastResult="
                + describe(lastResult));
    }

    private static ThroughputResult runWindow(ServiceNode caller, InvokeOptions options, int senderThreads,
                                              int maxInFlight, Duration duration,
                                              boolean fastSender, boolean fastCallback) throws InterruptedException {
        AtomicLong sent = new AtomicLong();
        AtomicLong success = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong inFlight = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + duration.toNanos();
        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastRpcSender preparedSender = fastSender || fastCallback
                        ? caller.prepareFastRpcSender(CMD, ACT, null, options)
                        : null;
                ServiceNode.FastRpcCallback callback = new ServiceNode.FastRpcCallback() {
                    @Override
                    public void onResponse(boolean ok, cn.itsub.kynet.common.codec.RpcWireCodec.RpcResponseView response) {
                        inFlight.decrementAndGet();
                        if (ok) {
                            success.incrementAndGet();
                        } else {
                            failed.incrementAndGet();
                        }
                    }

                    @Override
                    public void onFailure(String errorCode, String errorMessage) {
                        inFlight.decrementAndGet();
                        failed.incrementAndGet();
                    }
                };
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        long currentInFlight = inFlight.incrementAndGet();
                        if (currentInFlight > maxInFlight) {
                            inFlight.decrementAndGet();
                            Thread.onSpinWait();
                            continue;
                        }

                        sent.incrementAndGet();
                        try {
                            if (fastCallback) {
                                preparedSender.invoke(callback);
                                continue;
                            }
                            CompletableFuture<ActionResult> future = preparedSender == null
                                    ? caller.invokeAsync(CMD, ACT, null, options)
                                    : preparedSender.invokeAsync();
                            future
                                    .whenComplete((result, throwable) -> {
                                        inFlight.decrementAndGet();
                                        if (throwable != null || result == null || !result.isSuccess()) {
                                            failed.incrementAndGet();
                                            return;
                                        }
                                        success.incrementAndGet();
                                    });
                        } catch (RuntimeException e) {
                            inFlight.decrementAndGet();
                            failed.incrementAndGet();
                        }
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "broker-rpc-throughput-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
        }

        sendersDone.await();
        long drainDeadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos()
                + options.getTimeout().toNanos();
        while (inFlight.get() > 0 && System.nanoTime() < drainDeadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(2);
        }

        double seconds = duration.toNanos() / 1_000_000_000.0;
        return new ThroughputResult(
                senderThreads,
                maxInFlight,
                seconds,
                sent.get(),
                success.get(),
                failed.get(),
                inFlight.get());
    }

    private static void printResult(String label, ThroughputResult result) {
        System.out.printf(
                "%s: duration=%.2fs, senderThreads=%d, maxInFlight=%d, sent=%d, success=%d, failed=%d, inFlightLeft=%d, sendQps=%.2f, successQps=%.2f%n",
                label,
                result.durationSeconds(),
                result.senderThreads(),
                result.maxInFlight(),
                result.sent(),
                result.success(),
                result.failed(),
                result.inFlightLeft(),
                result.sendQps(),
                result.successQps());
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

    private record ThroughputResult(int senderThreads, int maxInFlight, double durationSeconds,
                                    long sent, long success, long failed, long inFlightLeft) {
        private double sendQps() {
            return durationSeconds <= 0 ? 0 : sent / durationSeconds;
        }

        private double successQps() {
            return durationSeconds <= 0 ? 0 : success / durationSeconds;
        }
    }
}
