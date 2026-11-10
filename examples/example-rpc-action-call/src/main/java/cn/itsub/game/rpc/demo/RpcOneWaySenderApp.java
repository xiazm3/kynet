package cn.itsub.game.rpc.demo;

import cn.itsub.game.rpc.demo.message.RpcDemoRequest;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

@Slf4j
public class RpcOneWaySenderApp {
    private static final int CMD = 102;
    private static final int ACT = 1;
    private static final int DEFAULT_SENDER_THREADS = 10;
    private static final int DEFAULT_DURATION_SECONDS = 10;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(3);
    private static final RpcDemoRequest FIXED_REQUEST = new RpcDemoRequest("invoke-one-way");

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;
        int senderThreads = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_SENDER_THREADS;
        int durationSeconds = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_DURATION_SECONDS;

        ServiceNode client = new ServiceNode(host, port, "RpcOneWaySender");
        client.start();

        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + Duration.ofSeconds(durationSeconds).toNanos();
        AtomicLong totalSent = new AtomicLong();
        AtomicLong totalFailed = new AtomicLong();
        AtomicLongArray sentPerThread = new AtomicLongArray(senderThreads);
        AtomicLongArray failPerThread = new AtomicLongArray(senderThreads);
        CountDownLatch senderDone = new CountDownLatch(senderThreads);
        long[] sessionIds = new long[senderThreads];
        long[] userIds = new long[senderThreads];

        for (int i = 0; i < senderThreads; i++) {
            sessionIds[i] = 102_000L + i + 1;
            userIds[i] = i + 1L;
        }

        log.info("one-way sender started, senderThreads={}, durationSeconds={}", senderThreads, durationSeconds);

        for (int i = 0; i < senderThreads; i++) {
            int threadIndex = i;
            Thread thread = new Thread(() -> {
                try {
                    while (System.nanoTime() < deadlineNanos) {
                        try {
                            client.invokeOneWay(
                                    CMD,
                                    ACT,
                                    FIXED_REQUEST,
                                    InvokeOptions.builder()
                                            .sessionId(sessionIds[threadIndex])
                                            .userId(userIds[threadIndex])
                                            .timeout(REQUEST_TIMEOUT)
                                            .build()
                            );
                            sentPerThread.incrementAndGet(threadIndex);
                            totalSent.incrementAndGet();
                        } catch (Exception e) {
                            failPerThread.incrementAndGet(threadIndex);
                            totalFailed.incrementAndGet();
                        }
                    }
                } finally {
                    senderDone.countDown();
                }
            }, "one-way-sender-" + (i + 1));
            thread.start();
        }

        try {
            senderDone.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("One-way sender benchmark interrupted", e);
        } finally {
            client.shutdown();
        }

        double elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        for (int i = 0; i < senderThreads; i++) {
            long threadSent = sentPerThread.get(i);
            long threadFail = failPerThread.get(i);
            double threadQps = elapsedSeconds > 0 ? threadSent / elapsedSeconds : 0;
            log.info("thread-{} qps={}, sent={}, failed={}",
                    i + 1,
                    String.format("%.2f", threadQps),
                    threadSent,
                    threadFail);
        }

        double totalQps = elapsedSeconds > 0 ? totalSent.get() / elapsedSeconds : 0;
        log.info("one-way sender total qps={}, sent={}, failed={}, durationSec={}",
                String.format("%.2f", totalQps),
                totalSent.get(),
                totalFailed.get(),
                String.format("%.2f", elapsedSeconds));
    }
}
