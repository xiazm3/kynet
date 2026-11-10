package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class RpcDirectThroughputCallerApp {
    private static final int CMD = 99;
    private static final int ACT = 1;
    private static final int DEFAULT_SENDER_THREADS = 8;
    private static final int DEFAULT_MAX_IN_FLIGHT = 16_384;
    private static final int DEFAULT_DURATION_SECONDS = 10;

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;
        int senderThreads = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_SENDER_THREADS;
        int maxInFlight = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_MAX_IN_FLIGHT;
        int durationSeconds = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_DURATION_SECONDS;
        int timeoutMs = args.length > 5 ? Integer.parseInt(args[5]) : 3_000;
        boolean fastSender = args.length > 6 && Boolean.parseBoolean(args[6]);
        boolean fastCallback = args.length > 7 && Boolean.parseBoolean(args[7]);

        ServiceNode client = new ServiceNode(host, port, "RpcBrokerThroughputCaller");
        client.start();

        AtomicLong sent = new AtomicLong();
        AtomicLong success = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong inFlight = new AtomicLong();
        CountDownLatch sendersDone = new CountDownLatch(senderThreads);

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + Duration.ofSeconds(durationSeconds).toNanos();
        Duration timeout = Duration.ofMillis(timeoutMs);
        InvokeOptions options = InvokeOptions.builder().timeout(timeout).build();

        for (int i = 0; i < senderThreads; i++) {
            Thread thread = new Thread(() -> {
                ServiceNode.FastRpcSender preparedSender = fastSender || fastCallback
                        ? client.prepareFastRpcSender(CMD, ACT, null, options)
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
                        long current = inFlight.incrementAndGet();
                        if (current > maxInFlight) {
                            inFlight.decrementAndGet();
                            Thread.onSpinWait();
                            continue;
                        }
                        sent.incrementAndGet();
                        if (fastCallback) {
                            preparedSender.invoke(callback);
                            continue;
                        }
                        CompletableFuture<ActionResult> future = preparedSender == null
                                ? client.invokeAsync(CMD, ACT, null, options)
                                : preparedSender.invokeAsync();
                        future
                                .whenComplete((result, throwable) -> {
                                    inFlight.decrementAndGet();
                                    if (throwable != null) {
                                        failed.incrementAndGet();
                                        return;
                                    }
                                    if (result != null && result.isSuccess()) {
                                        success.incrementAndGet();
                                    } else {
                                        failed.incrementAndGet();
                                    }
                                });
                    }
                } finally {
                    sendersDone.countDown();
                }
            }, "rpc-throughput-sender-" + (i + 1));
            thread.start();
        }

        try {
            sendersDone.await();
            long drainDeadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (inFlight.get() > 0 && System.nanoTime() < drainDeadline) {
                Thread.sleep(2);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Throughput benchmark interrupted", e);
        } finally {
            client.shutdown();
        }

        double elapsedSeconds = durationSeconds;
        double successQps = success.get() / elapsedSeconds;
        double sendQps = sent.get() / elapsedSeconds;
        log.info("broker throughput result: senderThreads={}, maxInFlight={}, durationSec={}, fastSender={}, fastCallback={}, sent={}, success={}, failed={}, inFlightLeft={}, sendQps={}, successQps={}",
                senderThreads,
                maxInFlight,
                durationSeconds,
                fastSender,
                fastCallback,
                sent.get(),
                success.get(),
                failed.get(),
                inFlight.get(),
                String.format("%.2f", sendQps),
                String.format("%.2f", successQps));
    }
}
