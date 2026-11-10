package cn.itsub.game.rpc.demo;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 纯 Aeron IPC 微基准：一个 Publication、一个 Subscription、一个接收 poll 线程，
 * 不含 kynet 的 envelope、帧解析和 action 分发。
 *
 * <p>用途：先测出当前机器/驱动配置下 Aeron 本身的上限，再和
 * {@code ABOneWayPeakThroughputTest} 的结果对比，即可估算框架开销。
 *
 * <p>可调参数（JVM -D）：
 * <pre>
 *   kynet.rawAeron.payloadBytes   消息字节数，默认 200（约等于 kynet 64B body 的整帧大小）
 *   kynet.rawAeron.senderThreads  发送线程/Publication 数，默认 1
 *   kynet.rawAeron.warmupSeconds  预热秒数，默认 2
 *   kynet.rawAeron.durationSeconds 测量秒数，默认 5
 *   kynet.rawAeron.threadingMode  DEDICATED | SHARED，默认 DEDICATED
 * </pre>
 *
 * <p>注意：Aeron 官方宣传的千万级消息/秒通常还叠加了 Linux、CPU 绑核/隔离、巨页、
 * busy-spin、极小消息等条件；本测试只代表当前这台 Windows 开发机的结果。
 */
class RawAeronIpcThroughputTest {
    private static final int STREAM_BASE = 900;

    @Test
    @Timeout(300)
    void measuresRawAeronIpcFragmentRate() throws Exception {
        int payloadBytes = positiveIntProperty("kynet.rawAeron.payloadBytes", 200);
        int senderThreads = positiveIntProperty("kynet.rawAeron.senderThreads", 1);
        int warmupSeconds = nonNegativeIntProperty("kynet.rawAeron.warmupSeconds", 2);
        int durationSeconds = positiveIntProperty("kynet.rawAeron.durationSeconds", 5);
        String handlerMode = System.getProperty("kynet.rawAeron.handler", "count");
        boolean twoClients = Boolean.getBoolean("kynet.rawAeron.twoClients");
        ThreadingMode threadingMode = ThreadingMode.valueOf(
                System.getProperty("kynet.rawAeron.threadingMode", "DEDICATED").toUpperCase(java.util.Locale.ROOT));

        Path aeronDir = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-raw-aeron-" + UUID.randomUUID()).toAbsolutePath();
        Files.createDirectories(aeronDir);

        System.out.printf(
                "raw Aeron IPC: payloadBytes=%d, senderThreads=%d, handler=%s, twoClients=%s, termLength=%s, threadingMode=%s, warmup=%ds, duration=%ds%n",
                payloadBytes, senderThreads, handlerMode, twoClients,
                System.getProperty("kynet.rawAeron.termLength", "default"),
                threadingMode, warmupSeconds, durationSeconds);

        try (MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .aeronDirectoryName(aeronDir.toString())
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(threadingMode))) {
            try (Aeron aeron = Aeron.connect(new Aeron.Context()
                    .aeronDirectoryName(driver.aeronDirectoryName()))) {
                Aeron publicationAeron = twoClients
                        ? Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()))
                        : aeron;
                try {
                    if (warmupSeconds > 0) {
                        Result warmup = runWindow(publicationAeron, aeron, payloadBytes, senderThreads,
                                handlerMode, warmupSeconds);
                        printResult("warmup", warmup);
                    }
                    Result measured = runWindow(publicationAeron, aeron, payloadBytes, senderThreads,
                            handlerMode, durationSeconds);
                    printResult("measure", measured);
                    assertTrue(measured.sent() > 0, "at least one offer should succeed");
                    assertTrue(measured.received() > 0, "at least one fragment should be received");
                } finally {
                    if (publicationAeron != aeron) {
                        publicationAeron.close();
                    }
                }
            }
        }
    }

    private static Result runWindow(Aeron publicationAeron, Aeron subscriptionAeron, int payloadBytes,
                                    int senderThreads, String handlerMode, int seconds)
            throws InterruptedException {
        int[] streamIds = new int[senderThreads];
        Publication[] publications = new Publication[senderThreads];
        Subscription[] subscriptions = new Subscription[senderThreads];
        UnsafeBuffer[] buffers = new UnsafeBuffer[senderThreads];
        int termLength = Integer.getInteger("kynet.rawAeron.termLength", 0);
        String ipcChannel = termLength > 0 ? "aeron:ipc?term-length=" + termLength : "aeron:ipc";
        boolean extraUdp = Boolean.getBoolean("kynet.rawAeron.extraUdp");
        Subscription extraUdpSubscription = null;
        try {
            if (extraUdp) {
                int udpPort = cn.itsub.kynet.common.transport.AeronTransport.findAvailablePort();
                extraUdpSubscription = subscriptionAeron.addSubscription(
                        "aeron:udp?endpoint=0.0.0.0:" + udpPort, 9999);
            }
            for (int i = 0; i < senderThreads; i++) {
                streamIds[i] = STREAM_BASE + i;
                publications[i] = publicationAeron.addPublication(ipcChannel, streamIds[i]);
                subscriptions[i] = subscriptionAeron.addSubscription(ipcChannel, streamIds[i]);
                buffers[i] = new UnsafeBuffer(ByteBuffer.allocateDirect(payloadBytes));
            }
            awaitConnected(publications, subscriptions);

            LongAdder sent = new LongAdder();
            LongAdder backPressure = new LongAdder();
            LongAdder received = new LongAdder();
            CountDownLatch sendersDone = new CountDownLatch(senderThreads);

            long startNanos = System.nanoTime();
            long deadlineNanos = startNanos + TimeUnit.SECONDS.toNanos(seconds);

            FragmentHandler handler = switch (handlerMode) {
                case "noop" -> (buffer, offset, length, header) -> {
                };
                case "parse" -> (buffer, offset, length, header) -> {
                    // Touch the same header cache lines a kynet fast one-way frame reads.
                    int action = buffer.getInt(offset + 4);
                    long requestId = buffer.getLong(offset + 17);
                    long userId = buffer.getLong(offset + 33);
                    int bodyLength = buffer.getInt(offset + Math.max(0, payloadBytes - 4));
                    if ((action | (int) requestId | (int) userId | bodyLength) == Integer.MIN_VALUE) {
                        received.add(action);
                    }
                    received.increment();
                };
                default -> (buffer, offset, length, header) -> received.increment();
            };

            Subscription udpSubscription = extraUdpSubscription;
            Thread receiver = new Thread(() -> {
                while (System.nanoTime() < deadlineNanos + TimeUnit.SECONDS.toNanos(1)) {
                    int work = 0;
                    if (udpSubscription != null) {
                        work += udpSubscription.poll(handler, 1024);
                    }
                    for (Subscription subscription : subscriptions) {
                        work += subscription.poll(handler, 1024);
                    }
                    if (work == 0) {
                        Thread.onSpinWait();
                    }
                }
            }, "raw-aeron-receiver");
            receiver.setDaemon(true);
            receiver.start();

            for (int i = 0; i < senderThreads; i++) {
                int index = i;
                Thread sender = new Thread(() -> {
                    UnsafeBuffer buffer = buffers[index];
                    Publication publication = publications[index];
                    long spins = 0;
                    while (System.nanoTime() < deadlineNanos) {
                        long result = publication.offer(buffer, 0, payloadBytes);
                        if (result > 0) {
                            sent.increment();
                            spins = 0;
                        } else if (result == Publication.BACK_PRESSURED) {
                            backPressure.increment();
                            if ((++spins & 0x3FF) == 0) {
                                Thread.onSpinWait();
                            }
                        } else {
                            Thread.onSpinWait();
                        }
                    }
                    sendersDone.countDown();
                }, "raw-aeron-sender-" + (i + 1));
                sender.setDaemon(true);
                sender.start();
            }

            sendersDone.await();
            long sendEndNanos = System.nanoTime();

            // 等待接收端把在途 fragment 消费完。
            long lastTotal = received.sum();
            long quietSince = System.nanoTime();
            long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < drainDeadline) {
                TimeUnit.MILLISECONDS.sleep(20);
                long current = received.sum();
                if (current == lastTotal) {
                    if (System.nanoTime() - quietSince >= TimeUnit.MILLISECONDS.toNanos(200)) {
                        break;
                    }
                } else {
                    lastTotal = current;
                    quietSince = System.nanoTime();
                }
            }
            long drainEndNanos = System.nanoTime();
            receiver.join(2_000);

            double sendSeconds = Math.max(0.000001, (sendEndNanos - startNanos) / 1_000_000_000.0);
            double drainSeconds = Math.max(0.0, (drainEndNanos - sendEndNanos) / 1_000_000_000.0);
            return new Result(senderThreads, sendSeconds, drainSeconds, sent.sum(),
                    backPressure.sum(), received.sum());
        } finally {
            if (extraUdpSubscription != null) {
                extraUdpSubscription.close();
            }
            for (Subscription subscription : subscriptions) {
                if (subscription != null) {
                    subscription.close();
                }
            }
            for (Publication publication : publications) {
                if (publication != null) {
                    publication.close();
                }
            }
        }
    }

    private static void awaitConnected(Publication[] publications, Subscription[] subscriptions)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadlineNanos) {
            boolean connected = true;
            for (Publication publication : publications) {
                connected &= publication.isConnected();
            }
            for (Subscription subscription : subscriptions) {
                connected &= subscription.isConnected();
            }
            if (connected) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
        fail("Aeron publication/subscription did not connect within 10s");
    }

    private static void printResult(String label, Result result) {
        System.out.printf(
                "%s: senders=%d sendDuration=%.2fs drain=%.2fs sent=%d received=%d backPressure=%d "
                        + "sendQps=%.0f receiveQps=%.0f delivery=%.2f%%%n",
                label,
                result.senderThreads(),
                result.sendSeconds(),
                result.drainSeconds(),
                result.sent(),
                result.received(),
                result.backPressure(),
                result.sendQps(),
                result.receiveQps(),
                result.deliveryPercent());
    }

    private static int positiveIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value > 0 ? value : defaultValue;
    }

    private static int nonNegativeIntProperty(String name, int defaultValue) {
        int value = Integer.getInteger(name, defaultValue);
        return value >= 0 ? value : defaultValue;
    }

    private record Result(int senderThreads, double sendSeconds, double drainSeconds,
                          long sent, long backPressure, long received) {
        private double sendQps() {
            return sent / sendSeconds;
        }

        private double receiveQps() {
            return received / (sendSeconds + drainSeconds);
        }

        private double deliveryPercent() {
            return sent == 0 ? 0.0 : received * 100.0 / sent;
        }
    }
}
