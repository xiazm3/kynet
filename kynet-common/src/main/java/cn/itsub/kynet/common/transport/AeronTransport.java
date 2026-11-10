package cn.itsub.kynet.common.transport;

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class AeronTransport {
    public static final int BROKER_UP_STREAM_ID = 1001;
    public static final int BROKER_DOWN_STREAM_ID = 1002;
    public static final int BROKER_UP_PARTITION_BASE_STREAM_ID = 2000;
    // A driver's CnC file alone is ~46MB and term buffers add more; a mount with less free
    // space than this cannot host a driver, so resolution falls back to the temp directory.
    private static final long MIN_SHM_FREE_BYTES = 256L * 1024 * 1024;
    private static volatile String defaultBaseDirectory;
    private static final long OFFER_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int PARK_EVERY_ATTEMPTS = 1_000;
    private static final int DEFAULT_BROKER_INPUT_PARTITIONS = 1;
    // Aeron's IPC default term is 64MB, i.e. a 192MB log buffer per publication. Broker control
    // traffic is tiny (registrations, heartbeats and 48KB directory chunks), so use a small term.
    private static final int DEFAULT_BROKER_TERM_LENGTH = 4 * 1024 * 1024;
    private static final int MIN_BROKER_TERM_LENGTH = 256 * 1024;
    private static final int TERM_MIN_LENGTH = 64 * 1024;
    private static final int TERM_MAX_LENGTH = 1024 * 1024 * 1024;

    private AeronTransport() {
    }

    public static String udpChannel(String host, int port) {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }

    public static String ipcChannel() {
        return "aeron:ipc";
    }

    /**
     * Broker control term length. Aeron's IPC default is 64MB (a 192MB log buffer per
     * publication); the broker's registrations/heartbeats/directory chunks fit comfortably in
     * a few MB, so a smaller term keeps the broker's per-client footprint bounded.
     */
    public static int brokerTermLength() {
        return validatedTermLength(Math.max(MIN_BROKER_TERM_LENGTH,
                Integer.getInteger("kynet.broker.termLength", DEFAULT_BROKER_TERM_LENGTH)), "kynet.broker.termLength");
    }

    /**
     * Aeron term buffers must be a power of two between 64KB and 1GB; validate eagerly so a bad
     * configured value fails with a readable message instead of a low-level Aeron term-buffer
     * error when the publication is later added.
     */
    public static int validatedTermLength(int requested, String property) {
        if (requested < TERM_MIN_LENGTH || requested > TERM_MAX_LENGTH || Integer.bitCount(requested) != 1) {
            throw new IllegalArgumentException(
                    property + " must be a power of two between 64KB and 1GB, got: " + requested);
        }
        return requested;
    }

    /** Appends a term-length parameter unless the channel already specifies one. */
    public static String withTermLength(String channel, int termLength) {
        if (channel.contains("term-length=")) {
            return channel;
        }
        return channel + (channel.indexOf('?') >= 0 ? "|" : "?") + "term-length=" + termLength;
    }

    public static String aeronBaseDirectory() {
        String configured = System.getProperty("kynet.aeron.baseDir");
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        // Memoized so every path derived inside one process (local driver directory, IPC peer
        // markers) resolves identically even if free space on the chosen mount changes later.
        String cached = defaultBaseDirectory;
        if (cached == null) {
            cached = defaultAeronBaseDirectory();
            defaultBaseDirectory = cached;
        }
        return cached;
    }

    /**
     * Linux follows Aeron's own default of {@code /dev/shm} (tmpfs): the driver's CnC file and
     * term buffers are memory-mapped transport state, so a memory-backed filesystem removes disk
     * writeback from the latency path. Stock Docker containers ship a 64MB /dev/shm that cannot
     * hold a driver's ~46MB CnC file, so fall back to the JDK temp directory when the mount is
     * missing or too small. Windows has no tmpfs convention; its temp directory keeps launches
     * configuration-free (the old fixed "/opt/aeron" default also resolved against the current
     * drive letter there, which surprised Windows users).
     */
    private static String defaultAeronBaseDirectory() {
        if (!isWindows() && shmHasCapacity()) {
            return "/dev/shm";
        }
        return Path.of(System.getProperty("java.io.tmpdir"), "aeron").toString();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("windows");
    }

    private static boolean shmHasCapacity() {
        try {
            Path shm = Path.of("/dev/shm");
            return Files.isDirectory(shm)
                    && Files.getFileStore(shm).getUsableSpace() >= MIN_SHM_FREE_BYTES;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    public static boolean isLocalHost(String host) {
        return "127.0.0.1".equals(host) || "localhost".equalsIgnoreCase(host);
    }

    public static int brokerInputPartitionCount() {
        return Math.max(1, Integer.getInteger("kynet.broker.partitions", DEFAULT_BROKER_INPUT_PARTITIONS));
    }

    public static int brokerUpStreamId(int partition) {
        if (partition <= 0) {
            return BROKER_UP_STREAM_ID;
        }
        return BROKER_UP_PARTITION_BASE_STREAM_ID + (partition - 1);
    }

    public static int brokerInputPartition(long routingKey) {
        return brokerInputPartition(routingKey, brokerInputPartitionCount());
    }

    public static int brokerInputPartition(long routingKey, int partitionCount) {
        if (partitionCount <= 1) {
            return 0;
        }
        return Math.floorMod((int) (routingKey ^ (routingKey >>> 32)), partitionCount);
    }

    public static String brokerAeronDirectory(int port) {
        return aeronBaseDirectory() + "/kynet-aeron-broker-" + port;
    }

    public static int ipcReplyStreamId(int instanceId) {
        return (instanceId & 0x7FFFFFFF) | 0x10000;
    }

    public static int findAvailablePort() {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        } catch (SocketException e) {
            throw new IllegalStateException("Failed to allocate local UDP port", e);
        }
    }

    public static String localHostAddress() {
        String configured = System.getProperty("kynet.reply.host");
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (!networkInterface.isUp() || networkInterface.isLoopback() || networkInterface.isVirtual()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address && !address.isLoopbackAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (SocketException ignored) {
        }
        try {
            InetAddress local = InetAddress.getLocalHost();
            if (local instanceof Inet4Address && !local.isAnyLocalAddress()) {
                return local.getHostAddress();
            }
        } catch (IOException ignored) {
        }
        return "127.0.0.1";
    }

    public static String localHostAddressForReply() {
        String host = localHostAddress();
        if (host.contains(":")) {
            return "127.0.0.1";
        }
        return host;
    }

    /*public static UnsafeBuffer newBuffer() {
        return new UnsafeBuffer(ByteBuffer.allocateDirect(1024 * 1024));
    }*/

    public static void offer(Publication publication, UnsafeBuffer buffer, byte[] encoded, String targetDescription)
            throws IOException {
        buffer.putBytes(0, encoded);
        offerDirect(publication, buffer, 0, encoded.length, targetDescription);
    }

    public static void offerDirect(Publication publication, DirectBuffer buffer, int offset, int length,
                                   String targetDescription) throws IOException {
        long result = Publication.NOT_CONNECTED;
        int attempts = 0;
        long startNanos = System.nanoTime();
        long deadline = startNanos + OFFER_TIMEOUT_NANOS;
        while (System.nanoTime() < deadline) {
            attempts++;
            result = publication.offer(buffer, offset, length);
            if (result > 0) {
                return;
            }
            if (result == Publication.BACK_PRESSURED
                    || result == Publication.ADMIN_ACTION
                    || result == Publication.NOT_CONNECTED) {
                if (attempts % PARK_EVERY_ATTEMPTS == 0) {
                    Thread.yield();
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    continue;
                }
                Thread.onSpinWait();
                continue;
            }
            break;
        }

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        throw new IOException("Failed to publish Aeron message to " + targetDescription
                + ", result=" + result + ", attempts=" + attempts + ", elapsedMs=" + elapsedMs);
    }

    public static long tryOfferDirect(Publication publication, DirectBuffer buffer, int offset, int length) {
        return publication.offer(buffer, offset, length);
    }
}
