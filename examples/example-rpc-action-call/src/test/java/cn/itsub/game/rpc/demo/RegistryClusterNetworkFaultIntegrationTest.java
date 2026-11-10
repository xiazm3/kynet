package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.common.transport.AeronTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.DatagramChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RegistryClusterNetworkFaultIntegrationTest {
    private static final String BROKER_MAIN = "cn.itsub.kynet.registry.RegistryMain";
    private static final String PROVIDER_MAIN = "cn.itsub.game.rpc.demo.RpcActionProviderApp";
    private static final String CALLER_MAIN = "cn.itsub.game.rpc.demo.RpcActionCallerApp";
    private static final String ACTUAL_A = "127.0.0.11";
    private static final String ACTUAL_B = "127.0.0.12";
    private static final String PROXY_A = "127.0.0.21";
    private static final String PROXY_B = "127.0.0.22";
    private static final String DISCOVERY_PROXY = "127.0.0.30";

    @Test
    @Timeout(70)
    void discoverySurvivesGossipLossJitterPartitionAndHealingAcrossBrokerProcesses() throws Exception {
        Path runDirectory = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-network-fault-cluster-" + UUID.randomUUID()).toAbsolutePath();
        Path logDirectory = Files.createDirectories(runDirectory.resolve("logs"));
        Set<Integer> allocatedPorts = new HashSet<>();
        int brokerPortA = uniqueAvailablePort(allocatedPorts);
        int brokerPortB = uniqueAvailablePort(allocatedPorts);
        int clusterPortA = uniqueAvailablePort(allocatedPorts);
        int clusterPortB = uniqueAvailablePort(allocatedPorts);
        int discoveryPort = uniqueAvailablePort(allocatedPorts);

        FaultPolicy discoveryFault = new FaultPolicy();
        FaultPolicy controlFault = new FaultPolicy();
        FaultPolicy registryConnectionFault = new FaultPolicy();
        Process brokerA = null;
        Process brokerB = null;
        Process provider = null;
        try (UdpFaultRouter router = new UdpFaultRouter()) {
            router.addHub(DISCOVERY_PROXY, discoveryPort,
                    List.of(endpoint(ACTUAL_A, discoveryPort), endpoint(ACTUAL_B, discoveryPort)),
                    discoveryFault);
            router.addControlPair(
                    endpoint(PROXY_A, clusterPortA), endpoint(ACTUAL_A, clusterPortA),
                    endpoint(PROXY_B, clusterPortB), endpoint(ACTUAL_B, clusterPortB),
                    controlFault);
            router.addNat(PROXY_A, brokerPortA, endpoint(ACTUAL_A, brokerPortA), registryConnectionFault);
            router.addNat(PROXY_B, brokerPortB, endpoint(ACTUAL_B, brokerPortB), registryConnectionFault);
            router.start();

            Path brokerALog = logDirectory.resolve("broker-a.log");
            Path brokerBLog = logDirectory.resolve("broker-b.log");
            Path providerLog = logDirectory.resolve("provider.log");
            brokerA = startJavaProcess(brokerALog, brokerArguments(runDirectory, discoveryPort,
                    "network-a", ACTUAL_A, PROXY_A, clusterPortA, brokerPortA));
            brokerB = startJavaProcess(brokerBLog, brokerArguments(runDirectory, discoveryPort,
                    "network-b", ACTUAL_B, PROXY_B, clusterPortB, brokerPortB));
            waitForLog(brokerALog, "Registry TCP started", brokerA, Duration.ofSeconds(8));
            waitForLog(brokerBLog, "Registry TCP started", brokerB, Duration.ofSeconds(8));

            provider = startJavaProcess(providerLog,
                    clientArguments(runDirectory, PROVIDER_MAIN, ACTUAL_A, brokerPortA));
            waitForLog(providerLog, "RPC action provider started", provider, Duration.ofSeconds(8));
            waitForCallerSuccess(logDirectory, "baseline", runDirectory, ACTUAL_B, brokerPortB,
                    Duration.ofSeconds(8));

            controlFault.degrade(0.05, 5, 10);
            int degradedSuccesses = 0;
            for (int iteration = 0; iteration < 4; iteration++) {
                degradedSuccesses += runCaller(logDirectory.resolve("degraded-" + iteration + ".log"),
                        runDirectory, ACTUAL_B, brokerPortB);
            }
            assertEquals(20, degradedSuccesses,
                    "discovery should converge and direct RPCs should succeed despite gossip loss and jitter");
            assertTrue(controlFault.dropped.sum() > 0, "fault router must actually drop packets");
            assertTrue(controlFault.delayed.sum() > 0, "fault router must actually delay packets");

            registryConnectionFault.normal();
            discoveryFault.partition();
            controlFault.partition();
            registryConnectionFault.partition();
            TimeUnit.MILLISECONDS.sleep(1_500);
            int partitionSuccesses = runCaller(logDirectory.resolve("partitioned.log"),
                    runDirectory, ACTUAL_B, brokerPortB);
            assertEquals(0, partitionSuccesses,
                    "a new caller must not discover providers behind an isolated registry");
            assertTrue(brokerA.isAlive() && brokerB.isAlive() && provider.isAlive(),
                    "network partition must not terminate broker or provider processes");

            long healingStartedNanos = System.nanoTime();
            discoveryFault.normal();
            controlFault.normal();
            registryConnectionFault.normal();
            waitForCallerSuccess(logDirectory, "healed", runDirectory, ACTUAL_B, brokerPortB,
                    Duration.ofSeconds(5));
            long healingMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - healingStartedNanos);
            assertFalse(provider.waitFor(100, TimeUnit.MILLISECONDS),
                    "provider process must remain alive across network healing");

            System.out.printf(
                    "registry network faults: degradedSuccess=%d/20, droppedPackets=%d, delayedPackets=%d, "
                            + "partitionSuccess=0/5, healingMillis=%d%n",
                    degradedSuccesses, controlFault.dropped.sum(), controlFault.delayed.sum(), healingMillis);
        } finally {
            destroy(provider);
            destroy(brokerA);
            destroy(brokerB);
        }
    }

    private static List<String> brokerArguments(Path runDirectory, int discoveryPort, String nodeId,
                                                String actualHost, String proxyHost,
                                                int clusterPort, int brokerPort) {
        List<String> arguments = javaPrefix(runDirectory);
        arguments.add("-Dkynet.registry.cluster.enabled=true");
        arguments.add("-Dkynet.registry.cluster.secret=kynet-cluster-test-secret");
        arguments.add("-Dkynet.registry.clientBindHost=" + actualHost);
        arguments.add("-Dkynet.registry.clientHost=" + proxyHost);
        arguments.add("-Dkynet.registry.clusterBindHost=" + actualHost);
        arguments.add("-Dkynet.registry.clusterHost=" + proxyHost);
        arguments.add("-Dkynet.registry.discoveryBindHost=" + actualHost);
        arguments.add("-Dkynet.registry.discoveryAddress=" + DISCOVERY_PROXY);
        arguments.add("-Dkynet.registry.discoveryPort=" + discoveryPort);
        arguments.add("-Dkynet.registry.discoveryIntervalMs=100");
        arguments.add("-Dkynet.registry.gossipIntervalMs=100");
        arguments.add("-Dkynet.registry.suspectTimeoutMs=500");
        arguments.add("-Dkynet.registry.deadTimeoutMs=1000");
        arguments.add("-Dkynet.registry.nodeId=" + nodeId);
        arguments.add("-Dkynet.registry.clusterPort=" + clusterPort);
        arguments.add("-cp");
        arguments.add(testClasspath());
        arguments.add(BROKER_MAIN);
        arguments.add(String.valueOf(brokerPort));
        return arguments;
    }

    private static List<String> clientArguments(Path runDirectory, String mainClass,
                                                String host, int brokerPort) {
        List<String> arguments = javaPrefix(runDirectory);
        arguments.add("-cp");
        arguments.add(testClasspath());
        arguments.add(mainClass);
        arguments.add(host);
        arguments.add(String.valueOf(brokerPort));
        return arguments;
    }

    private static List<String> javaPrefix(Path runDirectory) {
        List<String> arguments = new ArrayList<>();
        arguments.add(javaExecutable());
        arguments.add("--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED");
        arguments.add("-Dkynet.aeron.baseDir=" + runDirectory);
        return arguments;
    }

    private static Process startJavaProcess(Path logFile, List<String> arguments) throws IOException {
        return new ProcessBuilder(arguments)
                .redirectErrorStream(true)
                .redirectOutput(logFile.toFile())
                .start();
    }

    private static void waitForCallerSuccess(Path logDirectory, String label, Path runDirectory,
                                             String host, int brokerPort, Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        int attempt = 0;
        while (System.nanoTime() < deadlineNanos) {
            Path logFile = logDirectory.resolve(label + "-" + attempt++ + ".log");
            if (runCaller(logFile, runDirectory, host, brokerPort) == 5) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        fail("caller did not recover before " + timeout);
    }

    private static int runCaller(Path logFile, Path runDirectory, String host, int brokerPort)
            throws Exception {
        Process caller = startJavaProcess(logFile,
                clientArguments(runDirectory, CALLER_MAIN, host, brokerPort));
        assertTrue(caller.waitFor(10, TimeUnit.SECONDS), "caller process timed out");
        String output = readLog(logFile);
        assertEquals(0, caller.exitValue(), "caller process failed:\n" + output);
        return occurrences(output, "[success-case] status=");
    }

    private static void waitForLog(Path logFile, String expected, Process process, Duration timeout)
            throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (readLog(logFile).contains(expected)) {
                return;
            }
            if (!process.isAlive()) {
                fail("child process exited before log appeared: " + expected + "\n" + readLog(logFile));
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        fail("timed out waiting for child log: " + expected + "\n" + readLog(logFile));
    }

    private static String readLog(Path logFile) throws IOException {
        return Files.exists(logFile) ? Files.readString(logFile) : "";
    }

    private static int occurrences(String value, String expected) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(expected, offset)) >= 0) {
            count++;
            offset += expected.length();
        }
        return count;
    }

    private static InetSocketAddress endpoint(String host, int port) {
        return new InetSocketAddress(host, port);
    }

    private static int uniqueAvailablePort(Set<Integer> allocatedPorts) {
        int port;
        do {
            port = AeronTransport.findAvailablePort();
        } while (!allocatedPorts.add(port));
        return port;
    }

    private static String javaExecutable() {
        String executable = System.getProperty("os.name", "").startsWith("Windows") ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executable).toString();
    }

    private static String testClasspath() {
        String classpath = System.getProperty("surefire.test.class.path");
        return classpath == null || classpath.isBlank()
                ? System.getProperty("java.class.path")
                : classpath;
    }

    private static void destroy(Process process) {
        if (process == null || !process.isAlive()) {
            return;
        }
        process.destroyForcibly();
        try {
            process.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class FaultPolicy {
        private final LongAdder dropped = new LongAdder();
        private final LongAdder delayed = new LongAdder();
        private volatile boolean partitioned;
        private volatile double lossRate;
        private volatile int delayMillis;
        private volatile int jitterMillis;

        private void normal() {
            partitioned = false;
            lossRate = 0;
            delayMillis = 0;
            jitterMillis = 0;
        }

        private void partition() {
            partitioned = true;
        }

        private void degrade(double lossRate, int delayMillis, int jitterMillis) {
            this.partitioned = false;
            this.lossRate = lossRate;
            this.delayMillis = delayMillis;
            this.jitterMillis = jitterMillis;
        }
    }

    private static final class UdpFaultRouter implements AutoCloseable {
        private final List<ManagedRelay> relays = new ArrayList<>();

        private void addHub(String listenHost, int listenPort, List<InetSocketAddress> targets,
                            FaultPolicy policy) throws IOException {
            relays.add(new Relay(endpoint(listenHost, listenPort), targets, null, false, policy));
        }

        private void addControlPair(InetSocketAddress proxyA, InetSocketAddress actualA,
                                    InetSocketAddress proxyB, InetSocketAddress actualB,
                                    FaultPolicy policy) throws IOException {
            relays.add(new ControlPair(proxyA, actualA, proxyB, actualB, policy));
        }

        private void addNat(String listenHost, int listenPort, InetSocketAddress target,
                            FaultPolicy policy) throws IOException {
            relays.add(new Relay(endpoint(listenHost, listenPort), List.of(target), target, true, policy));
        }

        private void start() {
            relays.forEach(ManagedRelay::start);
        }

        @Override
        public void close() {
            relays.forEach(ManagedRelay::close);
        }
    }

    private interface ManagedRelay extends AutoCloseable {
        void start();

        @Override
        void close();
    }

    /**
     * Preserves each broker's advertised source endpoint while forwarding to the peer's real bind address.
     */
    private static final class ControlPair implements ManagedRelay {
        private final DatagramChannel channelA;
        private final DatagramChannel channelB;
        private final InetSocketAddress actualA;
        private final InetSocketAddress actualB;
        private final FaultPolicy policy;
        private final ScheduledExecutorService sender = Executors.newScheduledThreadPool(2);
        private final AtomicBoolean running = new AtomicBoolean();
        private final SplittableRandom random = new SplittableRandom(0x434f4e54524f4cL);
        private final List<Thread> threads = new ArrayList<>();

        private ControlPair(InetSocketAddress proxyA, InetSocketAddress actualA,
                            InetSocketAddress proxyB, InetSocketAddress actualB,
                            FaultPolicy policy) throws IOException {
            this.channelA = bind(proxyA);
            this.channelB = bind(proxyB);
            this.actualA = actualA;
            this.actualB = actualB;
            this.policy = policy;
        }

        @Override
        public void start() {
            running.set(true);
            threads.add(startReader(channelA, channelB, actualA, "a"));
            threads.add(startReader(channelB, channelA, actualB, "b"));
        }

        private Thread startReader(DatagramChannel receiveChannel, DatagramChannel sourceChannel,
                                   InetSocketAddress destination, String suffix) {
            Thread thread = new Thread(
                    () -> readLoop(receiveChannel, sourceChannel, destination),
                    "udp-control-router-" + suffix);
            thread.setDaemon(true);
            thread.start();
            return thread;
        }

        private void readLoop(DatagramChannel receiveChannel, DatagramChannel sourceChannel,
                              InetSocketAddress destination) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(65_535);
            while (running.get()) {
                try {
                    buffer.clear();
                    if (receiveChannel.receive(buffer) == null) {
                        continue;
                    }
                    buffer.flip();
                    byte[] payload = new byte[buffer.remaining()];
                    buffer.get(payload);
                    scheduleSend(sourceChannel, destination, payload);
                } catch (AsynchronousCloseException ignored) {
                    return;
                } catch (IOException e) {
                    if (running.get()) {
                        throw new IllegalStateException("UDP control router failed", e);
                    }
                    return;
                }
            }
        }

        private void scheduleSend(DatagramChannel sourceChannel, InetSocketAddress destination,
                                  byte[] payload) {
            if (policy.partitioned || (policy.lossRate > 0 && random.nextDouble() < policy.lossRate)) {
                policy.dropped.increment();
                return;
            }
            int delay = policy.delayMillis;
            if (policy.jitterMillis > 0) {
                delay += random.nextInt(policy.jitterMillis + 1);
            }
            Runnable send = () -> send(sourceChannel, payload, destination);
            if (delay <= 0) {
                send.run();
            } else {
                policy.delayed.increment();
                sender.schedule(send, delay, TimeUnit.MILLISECONDS);
            }
        }

        private void send(DatagramChannel sourceChannel, byte[] payload,
                          InetSocketAddress destination) {
            if (!running.get()) {
                return;
            }
            try {
                sourceChannel.send(ByteBuffer.wrap(payload), destination);
            } catch (IOException e) {
                if (running.get()) {
                    throw new IllegalStateException("UDP control router send failed", e);
                }
            }
        }

        @Override
        public void close() {
            running.set(false);
            closeChannel(channelA);
            closeChannel(channelB);
            sender.shutdownNow();
            for (Thread thread : threads) {
                try {
                    thread.join(2_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private static DatagramChannel bind(InetSocketAddress address) throws IOException {
            DatagramChannel channel = DatagramChannel.open();
            channel.bind(address);
            return channel;
        }

        private static void closeChannel(DatagramChannel channel) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static final class Relay implements ManagedRelay {
        private final DatagramChannel channel;
        private final List<InetSocketAddress> targets;
        private final InetSocketAddress target;
        private final boolean nat;
        private final FaultPolicy policy;
        private final Set<SocketAddress> upstreams = new CopyOnWriteArraySet<>();
        private final ScheduledExecutorService sender = Executors.newScheduledThreadPool(2);
        private final AtomicBoolean running = new AtomicBoolean();
        private final SplittableRandom random = new SplittableRandom(0x4b594e4554L);
        private Thread thread;

        private Relay(InetSocketAddress listen, List<InetSocketAddress> targets,
                      InetSocketAddress target, boolean nat, FaultPolicy policy) throws IOException {
            this.targets = targets;
            this.target = target;
            this.nat = nat;
            this.policy = policy;
            this.channel = DatagramChannel.open();
            this.channel.bind(listen);
        }

        @Override
        public void start() {
            running.set(true);
            thread = new Thread(this::readLoop, "udp-fault-router-" + channel.socket().getLocalPort());
            thread.setDaemon(true);
            thread.start();
        }

        private void readLoop() {
            ByteBuffer buffer = ByteBuffer.allocateDirect(65_535);
            while (running.get()) {
                try {
                    buffer.clear();
                    SocketAddress source = channel.receive(buffer);
                    if (source == null) {
                        continue;
                    }
                    buffer.flip();
                    byte[] payload = new byte[buffer.remaining()];
                    buffer.get(payload);
                    route(source, payload);
                } catch (AsynchronousCloseException ignored) {
                    return;
                } catch (IOException e) {
                    if (running.get()) {
                        throw new IllegalStateException("UDP fault router failed", e);
                    }
                    return;
                }
            }
        }

        private void route(SocketAddress source, byte[] payload) {
            if (policy.partitioned || (policy.lossRate > 0 && random.nextDouble() < policy.lossRate)) {
                policy.dropped.increment();
                return;
            }
            List<? extends SocketAddress> destinations = targets;
            if (nat) {
                if (sameEndpoint(source, target)) {
                    destinations = List.copyOf(upstreams);
                } else {
                    upstreams.add(source);
                }
            }
            int delay = policy.delayMillis;
            if (policy.jitterMillis > 0) {
                delay += random.nextInt(policy.jitterMillis + 1);
            }
            for (SocketAddress destination : destinations) {
                if (destination.equals(source)) {
                    continue;
                }
                if (delay <= 0) {
                    send(payload, destination);
                } else {
                    policy.delayed.increment();
                    sender.schedule(() -> send(payload, destination), delay, TimeUnit.MILLISECONDS);
                }
            }
        }

        private void send(byte[] payload, SocketAddress destination) {
            if (!running.get()) {
                return;
            }
            try {
                channel.send(ByteBuffer.wrap(payload), destination);
            } catch (IOException ignored) {
                if (running.get()) {
                    throw new IllegalStateException("UDP fault router send failed", ignored);
                }
            }
        }

        private static boolean sameEndpoint(SocketAddress source, InetSocketAddress expected) {
            if (!(source instanceof InetSocketAddress actual)) {
                return false;
            }
            return actual.getPort() == expected.getPort()
                    && actual.getAddress().equals(expected.getAddress());
        }

        @Override
        public void close() {
            running.set(false);
            try {
                channel.close();
            } catch (IOException ignored) {
            }
            sender.shutdownNow();
            if (thread != null) {
                try {
                    thread.join(2_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
