package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.registry.RegistryServer;
import cn.itsub.kynet.common.codec.RpcWireCodec.RpcResponseView;
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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 服务 A 调用服务 B 的 RPC 往返延迟与 GC 基准。
 *
 * <p>测量内容：
 * <ul>
 *   <li>p50 / p90 / p99 / p99.9 / max 往返延迟；</li>
 *   <li>吞吐（完成请求数 / 发送窗口）；</li>
 *   <li>窗口内 GC 次数、GC 停顿时间、线程总分配字节数、堆使用变化。</li>
 * </ul>
 *
 * <p>关键点是 in-flight 窗口：inFlight=1 是闭环延迟；inFlight 越大，队列和批处理
 * 带来的排队延迟越明显，p99 会上升。用同一组参数对比不同版本才有意义。
 *
 * <p>可调参数（JVM -D）：
 * <pre>
 *   kynet.rpcLatency.inFlight        在途请求数，支持逗号列表，默认 1,64
 *   kynet.rpcLatency.senderThreads   调用线程数，默认 1
 *   kynet.rpcLatency.warmupSeconds   每轮预热秒数，默认 2
 *   kynet.rpcLatency.durationSeconds 每轮测量秒数，默认 5
 *   kynet.rpcLatency.timeoutMs       单次 RPC 超时，默认 3000
 *   kynet.rpcLatency.maxSamples      延迟样本环形缓冲大小（2 的幂），默认 1048576
 *   kynet.direct.termLength          直接传输 term 长度，默认 8MB；高吞吐建议 32-64MB
 * </pre>
 *
 * <p>注意：同 JVM、IPC、provider 使用 INLINE 空 action，因此这里测的是传输 + 分发 +
 * 响应编码的延迟下限，不包含真实业务逻辑和数据库访问。
 */
class ABRpcLatencyGcTest {
    private static final int CMD = 130;
    private static final int ACT = 1;

    @Test
    @Timeout(300)
    void measuresRpcLatencyAndGc() throws Exception {
        int[] inFlightList = parseIntList(System.getProperty("kynet.rpcLatency.inFlight", "1,64"));
        int senderThreads = positiveIntProperty("kynet.rpcLatency.senderThreads", 1);
        int warmupSeconds = nonNegativeIntProperty("kynet.rpcLatency.warmupSeconds", 2);
        int durationSeconds = positiveIntProperty("kynet.rpcLatency.durationSeconds", 5);
        int timeoutMs = positiveIntProperty("kynet.rpcLatency.timeoutMs", 3_000);
        int maxSamples = Integer.highestOneBit(
                Math.max(1024, Integer.getInteger("kynet.rpcLatency.maxSamples", 1 << 20)));

        Path aeronBaseDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-rpc-latency-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronBaseDir);
        String previousBaseDir = System.getProperty("kynet.aeron.baseDir");
        System.setProperty("kynet.aeron.baseDir", aeronBaseDir.toString());

        int port = AeronTransport.findAvailablePort();
        MediaDriver mediaDriver = null;
        RegistryServer brokerServer = null;
        ServiceNode provider = null;
        ServiceNode caller = null;
        try {
            mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .threadingMode(ThreadingMode.DEDICATED)
                    .aeronDirectoryName(AeronTransport.brokerAeronDirectory(port)));
            brokerServer = new RegistryServer(port);
            brokerServer.start();

            provider = new ServiceNode("127.0.0.1", port, "LatencyProvider");
            provider.start();
            new ActionBootstrap(provider).registerAction(new LatencyPingAction());

            caller = new ServiceNode("127.0.0.1", port, "LatencyCaller");
            caller.start();

            System.out.printf(
                    "RPC latency/GC: inFlight=%s threads=%d warmup=%ds duration=%ds timeout=%dms maxSamples=%d%n",
                    Arrays.toString(inFlightList), senderThreads, warmupSeconds, durationSeconds,
                    timeoutMs, maxSamples);

            waitForProvider(caller);

            for (int inFlight : inFlightList) {
                Window window = new Window(maxSamples);
                if (warmupSeconds > 0) {
                    runWindow(caller, provider.getInstanceId(), senderThreads, inFlight,
                            Duration.ofSeconds(warmupSeconds), timeoutMs, window);
                    System.out.printf("warmup: inFlight=%d responses=%d failed=%d%n",
                            inFlight, window.responses.sum(), window.failed.sum());
                    window.reset();
                }

                GcSnapshot before = GcSnapshot.capture();
                runWindow(caller, provider.getInstanceId(), senderThreads, inFlight,
                        Duration.ofSeconds(durationSeconds), timeoutMs, window);
                GcSnapshot after = GcSnapshot.capture();

                printWindow(inFlight, senderThreads, window, before, after);
                assertTrue(window.responses.sum() > 0, "at least one RPC should complete");
                assertTrue(window.failed.sum() < window.responses.sum(),
                        "most RPCs should succeed");
            }
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
            if (previousBaseDir == null) {
                System.clearProperty("kynet.aeron.baseDir");
            } else {
                System.setProperty("kynet.aeron.baseDir", previousBaseDir);
            }
        }
    }

    private static void runWindow(ServiceNode caller, int targetInstanceId, int senderThreads,
                                  int inFlight, Duration duration, int timeoutMs, Window window)
            throws InterruptedException {
        ArrayBlockingQueue<CallbackSlot> slots = new ArrayBlockingQueue<>(inFlight);
        for (int i = 0; i < inFlight; i++) {
            slots.add(new CallbackSlot(window, slots));
        }
        CountDownLatch ready = new CountDownLatch(senderThreads);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>(senderThreads);

        for (int i = 0; i < senderThreads; i++) {
            int threadIndex = i;
            Thread thread = new Thread(() -> {
                InvokeOptions options = InvokeOptions.builder()
                        .targetInstanceId(targetInstanceId)
                        .sessionId(130_000L + threadIndex)
                        .userId(threadIndex + 1L)
                        .timeout(Duration.ofMillis(timeoutMs))
                        .build();
                ServiceNode.FastRpcSender sender = caller.prepareFastRpcSender(CMD, ACT, null, options);
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (true) {
                    long now = System.nanoTime();
                    if (now >= window.deadlineNanos) {
                        return;
                    }
                    CallbackSlot slot;
                    try {
                        slot = slots.poll(10, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if (slot == null) {
                        continue;
                    }
                    slot.markInUse();
                    slot.sendNanos = System.nanoTime();
                    try {
                        sender.invoke(slot);
                    } catch (RuntimeException e) {
                        window.recordFailure();
                        slot.release();
                    }
                }
            }, "rpc-latency-sender-" + (i + 1));
            thread.setDaemon(true);
            thread.start();
            threads.add(thread);
        }

        ready.await();
        window.startNanos = System.nanoTime();
        window.deadlineNanos = window.startNanos + duration.toNanos();
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        window.sendEndNanos = System.nanoTime();

        long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (slots.size() < inFlight && System.nanoTime() < drainDeadline) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        window.endNanos = System.nanoTime();
    }

    private static void printWindow(int inFlight, int senderThreads, Window window,
                                    GcSnapshot before, GcSnapshot after) {
        long responses = window.responses.sum();
        long failed = window.failed.sum();
        double sendSeconds = Math.max(0.000001, (window.sendEndNanos - window.startNanos) / 1_000_000_000.0);
        double totalSeconds = Math.max(sendSeconds, (window.endNanos - window.startNanos) / 1_000_000_000.0);

        int totalSamples = window.sampleIndex.get();
        int used = Math.min(totalSamples, window.samples.length);
        long[] sorted = totalSamples <= window.samples.length
                ? Arrays.copyOf(window.samples, totalSamples)
                : window.samples.clone();
        Arrays.sort(sorted);

        long gcCount = after.collectionCount() - before.collectionCount();
        long gcTimeMs = after.collectionTimeMs() - before.collectionTimeMs();
        long allocated = before.allocatedBytes() >= 0 && after.allocatedBytes() >= 0
                ? after.allocatedBytes() - before.allocatedBytes() : -1;
        long heapDelta = after.heapUsedBytes() - before.heapUsedBytes();

        System.out.printf(
                "inFlight=%-4d threads=%d responses=%d failed=%d qps=%.0f "
                        + "p50=%.1fus p90=%.1fus p99=%.1fus p99.9=%.1fus max=%.2fms "
                        + "gcCount=%d gcTime=%dms alloc=%.1fMB allocRate=%.1fMB/s heapDelta=%.1fMB%n",
                inFlight,
                senderThreads,
                responses,
                failed,
                responses / sendSeconds,
                percentile(sorted, 0.50) / 1_000.0,
                percentile(sorted, 0.90) / 1_000.0,
                percentile(sorted, 0.99) / 1_000.0,
                percentile(sorted, 0.999) / 1_000.0,
                percentile(sorted, 1.0) / 1_000_000.0,
                gcCount,
                gcTimeMs,
                allocated / 1_000_000.0,
                allocated / 1_000_000.0 / totalSeconds,
                heapDelta / 1_000_000.0);

        if (used != totalSamples) {
            System.out.printf("  latency samples wrapped: kept latest %d of %d%n", used, totalSamples);
        }
    }

    private static long percentile(long[] sorted, double percentile) {
        if (sorted.length == 0) {
            return 0;
        }
        int index = (int) Math.ceil(percentile * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
    }

    private static void waitForProvider(ServiceNode caller) throws InterruptedException {
        InvokeOptions probe = InvokeOptions.builder().timeout(Duration.ofMillis(500)).build();
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        ActionResult last = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                last = caller.invoke(CMD, ACT, null, probe);
                if (last.isSuccess()) {
                    return;
                }
            } catch (RuntimeException ignored) {
            }
            TimeUnit.MILLISECONDS.sleep(25);
        }
        fail("action 130:1 was not ready, lastResult="
                + (last == null ? "null" : last.getErrorCode() + ":" + last.getErrorMessage()));
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
    }

    private static int nonNegativeIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value >= 0 ? value : defaultValue;
    }

    private static int[] parseIntList(String value) {
        String[] parts = value.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            int parsed = Integer.parseInt(parts[i].trim());
            if (parsed <= 0) {
                throw new IllegalArgumentException("in-flight must be positive: " + value);
            }
            result[i] = parsed;
        }
        return result;
    }

    @ActionController(cmd = CMD)
    private static final class LatencyPingAction {
        @ActionMethod(act = ACT, mode = ActionExecutionMode.INLINE)
        public byte[] ping(ActionContext context) {
            return null;
        }
    }

    private static final class CallbackSlot implements ServiceNode.FastRpcCallback {
        private final Window window;
        private final ArrayBlockingQueue<CallbackSlot> pool;
        private final AtomicBoolean inUse = new AtomicBoolean();
        private volatile long sendNanos;

        private CallbackSlot(Window window, ArrayBlockingQueue<CallbackSlot> pool) {
            this.window = window;
            this.pool = pool;
        }

        private void markInUse() {
            inUse.set(true);
        }

        private void release() {
            if (inUse.compareAndSet(true, false)) {
                pool.offer(this);
            }
        }

        @Override
        public void onResponse(boolean success, RpcResponseView response) {
            long now = System.nanoTime();
            window.record(now - sendNanos, success, now);
            release();
        }

        @Override
        public void onFailure(String errorCode, String errorMessage) {
            window.recordFailure();
            release();
        }
    }

    private static final class Window {
        private final long[] samples;
        private final int mask;
        private final AtomicInteger sampleIndex = new AtomicInteger();
        private final LongAdder responses = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private volatile long startNanos;
        private volatile long deadlineNanos;
        private volatile long sendEndNanos;
        private volatile long endNanos;

        private Window(int capacity) {
            this.samples = new long[capacity];
            this.mask = capacity - 1;
        }

        private void record(long latencyNanos, boolean success, long completionNanos) {
            responses.increment();
            if (!success) {
                failed.increment();
                return;
            }
            // Only sample responses that completed inside the send window. Requests still
            // draining after the deadline would otherwise inflate the tail with shutdown queueing.
            if (completionNanos <= deadlineNanos) {
                int index = sampleIndex.getAndIncrement();
                samples[index & mask] = latencyNanos;
            }
        }

        private void recordFailure() {
            responses.increment();
            failed.increment();
        }

        private void reset() {
            sampleIndex.set(0);
            responses.reset();
            failed.reset();
        }
    }

    private record GcSnapshot(long collectionCount, long collectionTimeMs,
                              long allocatedBytes, long heapUsedBytes) {
        private static GcSnapshot capture() {
            long count = 0;
            long time = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long gcCount = gc.getCollectionCount();
                long gcTime = gc.getCollectionTime();
                if (gcCount > 0) {
                    count += gcCount;
                }
                if (gcTime > 0) {
                    time += gcTime;
                }
            }
            long allocated = -1;
            try {
                java.lang.management.ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
                if (threadBean instanceof com.sun.management.ThreadMXBean allocatedBean
                        && allocatedBean.isThreadAllocatedMemorySupported()) {
                    if (!allocatedBean.isThreadAllocatedMemoryEnabled()) {
                        allocatedBean.setThreadAllocatedMemoryEnabled(true);
                    }
                    allocated = allocatedBean.getTotalThreadAllocatedBytes();
                }
            } catch (RuntimeException ignored) {
            }
            long heapUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            return new GcSnapshot(count, time, allocated, heapUsed);
        }
    }
}
