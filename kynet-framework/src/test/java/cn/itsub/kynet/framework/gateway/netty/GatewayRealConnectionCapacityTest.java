package cn.itsub.kynet.framework.gateway.netty;

import cn.itsub.kynet.common.codec.BroadcastPayloadCodec;
import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.protocol.GatewayPushMessage;
import cn.itsub.kynet.common.protocol.PlayerBroadcastEnvelope;
import cn.itsub.kynet.common.util.ActionKey;
import cn.itsub.kynet.framework.client.ServiceNode;
import cn.itsub.kynet.framework.gateway.GatewayEndpoint;
import cn.itsub.kynet.framework.gateway.GatewayPacket;
import cn.itsub.kynet.framework.gateway.GatewayPacketCodec;
import cn.itsub.kynet.framework.gateway.GatewayPacketType;
import cn.itsub.kynet.framework.gateway.GatewayRuntime;
import cn.itsub.kynet.framework.gateway.GatewayServer;
import cn.itsub.kynet.framework.gateway.GatewayTransport;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class GatewayRealConnectionCapacityTest {
    private static final long MIB = 1024L * 1024L;
    private static final int BROADCAST_CMD = 300;
    private static final int BROADCAST_ACT = 1;

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void holdsTenThousandRealTcpConnectionsThroughReconnectAndBroadcast() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("kynet.gateway.capacity.enabled"),
                "Enable explicitly with -Dkynet.gateway.capacity.enabled=true");

        int connections = positiveInt("kynet.gateway.capacity.connections", 10_000);
        int reconnects = positiveInt("kynet.gateway.capacity.reconnects", 1_000);
        int workers = positiveInt("kynet.gateway.capacity.workers", 64);
        assertTrue(reconnects < connections, "reconnects must be smaller than connections");
        Path summaryFile = Path.of(System.getProperty("kynet.gateway.capacity.summaryFile",
                "target/gateway-real-connection-capacity-summary.txt")).toAbsolutePath();
        int port = availablePort();

        StubServiceNode brokerClient = new StubServiceNode();
        GatewayServer server = new GatewayServer(brokerClient, "tcp://127.0.0.1:" + port);
        server.setMaxConnections(connections + workers);
        server.setMaxConnectionsPerIp(0);
        server.setMaxConnectionAttemptsPerIpPerSecond(0);
        server.setSessionIdleTimeoutSeconds(600);
        GatewayRuntime runtime = new GatewayRuntime(server);
        GatewayTransport transport = NettyGatewayTransportFactory.create(
                GatewayEndpoint.parse("tcp://127.0.0.1:" + port), runtime);
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        Socket[] sockets = new Socket[connections];
        long heapBaseline = usedHeapAfterGc();
        long connectNanos = 0L;
        long pingNanos = 0L;
        long disconnectNanos = 0L;
        long reconnectNanos = 0L;
        long broadcastNanos = 0L;

        try {
            runtime.start();
            transport.start();

            long started = System.nanoTime();
            parallelRange(executor, 0, connections,
                    index -> sockets[index] = connectWithRetry(port, Duration.ofSeconds(60)));
            waitForCondition(() -> runtime.onlineConnectionCount() == connections,
                    Duration.ofSeconds(60), "gateway did not register all initial connections");
            connectNanos = System.nanoTime() - started;

            byte[] pingFrame = encodeFrame(new GatewayPacket(
                    GatewayPacketType.PING, 1L, 0, 0, false, 0L,
                    0L, 0L, null, null, false, null, null));
            started = System.nanoTime();
            parallelRange(executor, 0, connections, index -> {
                sockets[index].getOutputStream().write(pingFrame);
                sockets[index].getOutputStream().flush();
                GatewayPacket pong = readPacket(sockets[index]);
                assertEquals(GatewayPacketType.PONG, pong.type(), "connection " + index + " did not pong");
                assertEquals(1L, pong.requestId(), "connection " + index + " pong request id mismatch");
            });
            pingNanos = System.nanoTime() - started;

            started = System.nanoTime();
            parallelRange(executor, 0, reconnects, index -> closeSocket(sockets[index]));
            waitForCondition(() -> runtime.onlineConnectionCount() == connections - reconnects,
                    Duration.ofSeconds(60), "gateway did not unregister disconnected clients");
            disconnectNanos = System.nanoTime() - started;

            started = System.nanoTime();
            parallelRange(executor, 0, reconnects,
                    index -> sockets[index] = connectWithRetry(port, Duration.ofSeconds(60)));
            waitForCondition(() -> runtime.onlineConnectionCount() == connections,
                    Duration.ofSeconds(60), "gateway did not register replacement connections");
            reconnectNanos = System.nanoTime() - started;

            byte[] expectedBroadcast = BroadcastPayloadCodec.encode("gateway-capacity-broadcast");
            PlayerBroadcastEnvelope envelope = new PlayerBroadcastEnvelope();
            envelope.setMsgId(ActionKey.of(BROADCAST_CMD, BROADCAST_ACT));
            envelope.setPayload(expectedBroadcast);
            GatewayPushMessage push = new GatewayPushMessage();
            push.setCmd(3);
            push.setAct(1);
            push.setPayload(ProtobufPayloadCodec.encode(envelope));

            started = System.nanoTime();
            runtime.handleBrokerPush(push);
            parallelRange(executor, 0, connections, index -> {
                GatewayPacket packet = readPacket(sockets[index]);
                assertEquals(GatewayPacketType.PUSH, packet.type(),
                        "connection " + index + " did not receive broadcast push");
                assertEquals(BROADCAST_CMD, packet.cmd());
                assertEquals(BROADCAST_ACT, packet.act());
                assertArrayEquals(expectedBroadcast, packet.payload());
            });
            waitForCondition(() -> runtime.pendingOutboundMessages() == 0
                            && runtime.pendingOutboundBytes() == 0,
                    Duration.ofSeconds(30), "gateway outbound queue did not drain");
            broadcastNanos = System.nanoTime() - started;

            assertEquals(0L, runtime.rejectedConnectionCount(), "gateway rejected a capacity-test connection");
            assertEquals(0L, runtime.outboundBackpressureCount(), "gateway entered outbound backpressure");
            assertEquals(0L, runtime.outboundDroppedCount(), "gateway dropped an outbound packet");
            assertEquals(0L, runtime.slowClientClosedCount(), "gateway closed a test client as slow");

            long heapAtCapacity = usedHeapAfterGc();
            assertTrue(heapAtCapacity < ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * 9 / 10,
                    "gateway connection test exceeded 90% of maximum heap");
            writeSummary(summaryFile, "PASS", connections, reconnects, workers,
                    connectNanos, pingNanos, disconnectNanos, reconnectNanos, broadcastNanos,
                    runtime, heapBaseline, heapAtCapacity, null);
            System.out.printf(Locale.ROOT,
                    "GATEWAY_REAL_CONNECTION_CAPACITY PASS connections=%d connectSeconds=%.3f "
                            + "pingPongSeconds=%.3f reconnects=%d reconnectSeconds=%.3f "
                            + "broadcastSeconds=%.3f heapBaselineMiB=%d heapAtCapacityMiB=%d%n",
                    connections, seconds(connectNanos), seconds(pingNanos), reconnects,
                    seconds(reconnectNanos), seconds(broadcastNanos), heapBaseline / MIB, heapAtCapacity / MIB);
        } catch (Throwable failure) {
            writeSummary(summaryFile, "FAIL", connections, reconnects, workers,
                    connectNanos, pingNanos, disconnectNanos, reconnectNanos, broadcastNanos,
                    runtime, heapBaseline, usedHeap(), failure);
            throw failure;
        } finally {
            for (Socket socket : sockets) {
                closeSocket(socket);
            }
            try {
                waitForCondition(() -> runtime.onlineConnectionCount() == 0,
                        Duration.ofSeconds(30), "gateway sessions remained after client shutdown");
            } finally {
                executor.shutdownNow();
                transport.close();
                runtime.close();
            }
        }
    }

    private static void parallelRange(
            ExecutorService executor, int fromInclusive, int toExclusive, IndexedTask task) throws Exception {
        List<Callable<Void>> calls = new ArrayList<>(toExclusive - fromInclusive);
        for (int index = fromInclusive; index < toExclusive; index++) {
            int current = index;
            calls.add(() -> {
                task.run(current);
                return null;
            });
        }
        List<Future<Void>> results = executor.invokeAll(calls);
        for (Future<Void> result : results) {
            result.get();
        }
    }

    private static Socket connectWithRetry(int port, Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        IOException lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            Socket socket = new Socket();
            try {
                socket.setTcpNoDelay(true);
                socket.setReceiveBufferSize(8 * 1024);
                socket.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
                socket.setSoTimeout(30_000);
                return socket;
            } catch (IOException failure) {
                lastFailure = failure;
                closeSocket(socket);
                TimeUnit.MILLISECONDS.sleep(50);
            }
        }
        throw new IOException("timed out connecting to gateway", lastFailure);
    }

    private static byte[] encodeFrame(GatewayPacket packet) {
        ByteBuf buffer = Unpooled.buffer(Integer.BYTES + GatewayPacketCodec.encodedLength(packet));
        try {
            buffer.writeInt(GatewayPacketCodec.encodedLength(packet));
            GatewayPacketCodec.encode(buffer, packet);
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.readBytes(bytes);
            return bytes;
        } finally {
            buffer.release();
        }
    }

    private static GatewayPacket readPacket(Socket socket) throws IOException {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int length = input.readInt();
        if (length <= 0 || length > 1024 * 1024) {
            throw new IOException("invalid gateway frame length: " + length);
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
        try {
            return GatewayPacketCodec.decode(buffer);
        } finally {
            buffer.release();
        }
    }

    private static void waitForCondition(BooleanSupplier condition, Duration timeout, String message)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }
        fail(message);
    }

    private static int availablePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    private static int positiveInt(String property, int defaultValue) {
        int value = Integer.getInteger(property, defaultValue);
        if (value <= 0) {
            throw new IllegalArgumentException(property + " must be positive");
        }
        return value;
    }

    private static void closeSocket(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        for (int attempt = 0; attempt < 2; attempt++) {
            System.gc();
            TimeUnit.MILLISECONDS.sleep(250);
        }
        return usedHeap();
    }

    private static long usedHeap() {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        return memory.getHeapMemoryUsage().getUsed();
    }

    private static double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

    private static void writeSummary(
            Path path,
            String result,
            int connections,
            int reconnects,
            int workers,
            long connectNanos,
            long pingNanos,
            long disconnectNanos,
            long reconnectNanos,
            long broadcastNanos,
            GatewayRuntime runtime,
            long heapBaseline,
            long heapAtCapacity,
            Throwable failure) {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            String text = "result=" + result + '\n'
                    + "connections=" + connections + '\n'
                    + "reconnects=" + reconnects + '\n'
                    + "workers=" + workers + '\n'
                    + "connectSeconds=" + seconds(connectNanos) + '\n'
                    + "connectRatePerSecond=" + (connectNanos == 0 ? 0 : connections / seconds(connectNanos)) + '\n'
                    + "pingPongSeconds=" + seconds(pingNanos) + '\n'
                    + "disconnectSeconds=" + seconds(disconnectNanos) + '\n'
                    + "reconnectSeconds=" + seconds(reconnectNanos) + '\n'
                    + "broadcastSeconds=" + seconds(broadcastNanos) + '\n'
                    + "rejectedConnections=" + runtime.rejectedConnectionCount() + '\n'
                    + "outboundBackpressure=" + runtime.outboundBackpressureCount() + '\n'
                    + "outboundDropped=" + runtime.outboundDroppedCount() + '\n'
                    + "slowClientClosed=" + runtime.slowClientClosedCount() + '\n'
                    + "pendingOutboundMessages=" + runtime.pendingOutboundMessages() + '\n'
                    + "pendingOutboundBytes=" + runtime.pendingOutboundBytes() + '\n'
                    + "onlineConnectionsAtSummary=" + runtime.onlineConnectionCount() + '\n'
                    + "heapBaselineMiB=" + heapBaseline / MIB + '\n'
                    + "heapAtCapacityMiB=" + heapAtCapacity / MIB + '\n'
                    + (failure == null ? "" : "failure=" + failure.getClass().getName() + ":"
                            + String.valueOf(failure.getMessage()).replace('\n', ' ') + '\n');
            Files.writeString(path, text, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException ignored) {
        }
    }

    @FunctionalInterface
    private interface IndexedTask {
        void run(int index) throws Exception;
    }

    private static final class StubServiceNode extends ServiceNode {
        private StubServiceNode() {
            super("127.0.0.1", 1, "GatewayRealConnectionCapacityTest");
        }
    }
}
