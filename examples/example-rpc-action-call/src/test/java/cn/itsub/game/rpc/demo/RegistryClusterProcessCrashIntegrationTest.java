package cn.itsub.game.rpc.demo;

import cn.itsub.kynet.common.transport.AeronTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RegistryClusterProcessCrashIntegrationTest {
    private static final String BROKER_MAIN = "cn.itsub.kynet.registry.RegistryMain";
    private static final String PROVIDER_MAIN = "cn.itsub.game.rpc.demo.RpcActionProviderApp";
    private static final String CALLER_MAIN = "cn.itsub.game.rpc.demo.RpcActionCallerApp";

    @Test
    @Timeout(30)
    void providerProcessReconnectsAfterBrokerProcessIsForciblyTerminated() throws Exception {
        Path runDirectory = Path.of(System.getProperty("java.io.tmpdir"),
                "kynet-process-cluster-" + UUID.randomUUID()).toAbsolutePath();
        Path logDirectory = Files.createDirectories(runDirectory.resolve("logs"));
        Set<Integer> allocatedPorts = new HashSet<>();
        int brokerPortA = uniqueAvailablePort(allocatedPorts);
        int brokerPortB = uniqueAvailablePort(allocatedPorts);
        int clusterPortA = uniqueAvailablePort(allocatedPorts);
        int clusterPortB = uniqueAvailablePort(allocatedPorts);
        int discoveryPort = uniqueAvailablePort(allocatedPorts);

        Process brokerA = null;
        Process brokerB = null;
        Process provider = null;
        try {
            Path brokerALog = logDirectory.resolve("broker-a.log");
            Path brokerBLog = logDirectory.resolve("broker-b.log");
            Path providerLog = logDirectory.resolve("provider.log");
            brokerA = startJavaProcess(brokerALog, brokerArguments(
                    runDirectory, discoveryPort, "process-a", clusterPortA, brokerPortA));
            brokerB = startJavaProcess(brokerBLog, brokerArguments(
                    runDirectory, discoveryPort, "process-b", clusterPortB, brokerPortB));
            waitForLog(brokerALog, "Registry TCP started", brokerA, Duration.ofSeconds(8));
            waitForLog(brokerBLog, "Registry TCP started", brokerB, Duration.ofSeconds(8));

            provider = startJavaProcess(providerLog, clientArguments(
                    runDirectory, PROVIDER_MAIN, brokerPortA));
            waitForLog(providerLog, "RPC action provider started", provider, Duration.ofSeconds(8));
            TimeUnit.SECONDS.sleep(1);
            assertCallerSuccess(logDirectory.resolve("caller-before.log"), runDirectory, brokerPortB);

            long failedBrokerPid = brokerA.pid();
            long failureStartedNanos = System.nanoTime();
            brokerA.destroyForcibly();
            assertTrue(brokerA.waitFor(5, TimeUnit.SECONDS), "failed broker process should terminate");
            brokerA = null;

            waitForLog(providerLog, "connected to registry endpoint=127.0.0.1:" + brokerPortB,
                    provider, Duration.ofSeconds(8));
            long reconnectMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - failureStartedNanos);
            assertFalse(provider.waitFor(100, TimeUnit.MILLISECONDS),
                    "provider process must survive its registry process loss");
            assertCallerSuccess(logDirectory.resolve("caller-after.log"), runDirectory, brokerPortB);

            long restartStartedNanos = System.nanoTime();
            brokerA = startJavaProcess(logDirectory.resolve("broker-a-restarted.log"), brokerArguments(
                    runDirectory, discoveryPort, "process-a", clusterPortA, brokerPortA));
            waitForLog(logDirectory.resolve("broker-a-restarted.log"),
                    "Registry TCP started", brokerA, Duration.ofSeconds(8));
            waitForCallerSuccess(logDirectory, "caller-after-restart", runDirectory,
                    brokerPortA, Duration.ofSeconds(5));
            long restartConvergenceMillis = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - restartStartedNanos);

            System.out.printf(
                    "broker process crash recovery: failedPid=%d, restartedPid=%d, survivorPid=%d, "
                            + "providerPid=%d, reconnectMillis=%d, restartConvergenceMillis=%d%n",
                    failedBrokerPid, brokerA.pid(), brokerB.pid(), provider.pid(), reconnectMillis,
                    restartConvergenceMillis);
        } finally {
            destroy(provider);
            destroy(brokerA);
            destroy(brokerB);
        }
    }

    private static List<String> brokerArguments(Path runDirectory, int discoveryPort,
                                                String nodeId, int clusterPort, int brokerPort) {
        List<String> arguments = javaPrefix(runDirectory);
        arguments.add("-Dkynet.registry.cluster.enabled=true");
        arguments.add("-Dkynet.registry.cluster.secret=kynet-cluster-test-secret");
        arguments.add("-Dkynet.registry.clusterHost=127.0.0.1");
        arguments.add("-Dkynet.registry.clusterBindHost=127.0.0.1");
        arguments.add("-Dkynet.registry.clientHost=127.0.0.1");
        arguments.add("-Dkynet.registry.discoveryAddress=255.255.255.255");
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

    private static List<String> clientArguments(Path runDirectory, String mainClass, int brokerPort) {
        List<String> arguments = javaPrefix(runDirectory);
        arguments.add("-cp");
        arguments.add(testClasspath());
        arguments.add(mainClass);
        arguments.add("127.0.0.1");
        arguments.add(String.valueOf(brokerPort));
        return arguments;
    }

    private static List<String> javaPrefix(Path runDirectory) {
        List<String> arguments = new ArrayList<>();
        arguments.add(javaExecutable());
        arguments.add("--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED");
        arguments.add("-Xms256m");
        arguments.add("-Xmx256m");
        arguments.add("-Dkynet.aeron.baseDir=" + runDirectory);
        return arguments;
    }

    private static Process startJavaProcess(Path logFile, List<String> arguments) throws IOException {
        return new ProcessBuilder(arguments)
                .redirectErrorStream(true)
                .redirectOutput(logFile.toFile())
                .start();
    }

    private static void assertCallerSuccess(Path logFile, Path runDirectory, int brokerPort) throws Exception {
        assertEquals(5, runCaller(logFile, runDirectory, brokerPort),
                "caller should complete all RPCs:\n" + readLog(logFile));
    }

    private static void waitForCallerSuccess(Path logDirectory, String label, Path runDirectory,
                                             int brokerPort, Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        int attempt = 0;
        while (System.nanoTime() < deadlineNanos) {
            if (runCaller(logDirectory.resolve(label + "-" + attempt++ + ".log"),
                    runDirectory, brokerPort) == 5) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        fail("restarted broker did not converge before " + timeout);
    }

    private static int runCaller(Path logFile, Path runDirectory, int brokerPort) throws Exception {
        Process caller = startJavaProcess(logFile, clientArguments(runDirectory, CALLER_MAIN, brokerPort));
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
}
