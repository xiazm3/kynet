package cn.itsub.game.rpc.demo;

import cn.itsub.game.rpc.demo.message.RpcDemoRequest;
import cn.itsub.kynet.framework.action.ActionResult;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.client.InvokeOptions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RpcActionQpsBenchmarkApp {
    private static final int CMD = 10;
    private static final int ACT = 1;
    private static final int DEFAULT_TOTAL_CALLS = 10_000_000;
    private static final int DEFAULT_CONCURRENCY = 16_384;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8888;
        int totalCalls = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_TOTAL_CALLS;
        int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_CONCURRENCY;

        ServiceNode client = new ServiceNode(host, port, "RpcActionQpsBenchmark");
        client.start();

        try {
            runBenchmark(client, totalCalls, concurrency);
        } finally {
            client.shutdown();
        }
    }

    private static void runBenchmark(ServiceNode client, int totalCalls, int concurrency) {
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();
        Map<String, Integer> failureCounts = new LinkedHashMap<>();
        String firstFailure = null;
        long startNanos = System.nanoTime();
        InvokeOptions options = InvokeOptions.builder()
                .sessionId(10_001L)
                .userId(20001L)
                .timeout(TIMEOUT)
                .build();

        int batchSize = Math.max(1, concurrency);
        int submitted = 0;

        var request = new RpcDemoRequest("benchmark");

        while (submitted < totalCalls) {
            int currentBatchSize = Math.min(batchSize, totalCalls - submitted);
            List<CompletableFuture<ActionResult>> batch = new ArrayList<>(currentBatchSize);

            for (int i = 0; i < currentBatchSize; i++) {
                batch.add(client.invokeAsync(
                        CMD,
                        ACT,
                        request,
                        options
                ));
            }

            for (CompletableFuture<ActionResult> future : batch) {
                ActionResult result;
                try {
                    result = future.join();
                } catch (CompletionException e) {
                    failCount.incrementAndGet();
                    Throwable cause = rootCause(e);
                    String errorCode = "EXCEPTION:" + cause.getClass().getSimpleName();
                    failureCounts.merge(errorCode, 1, Integer::sum);
                    if (firstFailure == null) {
                        firstFailure = errorCode + ": " + cause.getMessage();
                    }
                    continue;
                }
                if (result.isSuccess()) {
                    successCount.incrementAndGet();
                } else {
                    failCount.incrementAndGet();
                    String errorCode = result.getErrorCode();
                    if (errorCode == null || errorCode.isBlank()) {
                        errorCode = "UNKNOWN";
                    }
                    failureCounts.merge(errorCode, 1, Integer::sum);
                    if (firstFailure == null) {
                        firstFailure = errorCode + ": " + result.getErrorMessage();
                    }
                }
            }

            submitted += currentBatchSize;
            if (submitted % 100_000 == 0 || submitted == totalCalls) {
                log.info("benchmark progress: {}/{}, concurrency={}", submitted, totalCalls, concurrency);
            }
        }

        long elapsedNanos = System.nanoTime() - startNanos;
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        double qps = elapsedSeconds > 0 ? successCount.get() / elapsedSeconds : 0;

        log.info(
                "benchmark done: totalCalls={}, success={}, failed={}, concurrency={}, elapsedMs={}, qps={}",
                totalCalls,
                successCount.get(),
                failCount.get(),
                concurrency,
                elapsedNanos / 1_000_000,
                String.format("%.2f", qps)
        );
        log.info("benchmark transport: ipcSent={}, udpSent={}, dropped={}",
                client.directIpcSentCount(), client.directUdpSentCount(), client.directDroppedCount());
        if (!failureCounts.isEmpty()) {
            log.info("benchmark failureCounts={}, firstFailure={}", failureCounts, firstFailure);
        }
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
