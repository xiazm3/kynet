package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.protocol.ServiceDirectoryCodec.Endpoint;
import cn.itsub.kynet.common.transport.AeronTransport;
import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** One I/O owner, bounded per-peer queues, and a driver independent of registry connections. */
final class PeerTransport implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PeerTransport.class);
    private static final int MAGIC = 0x4b594e31;
    private static final int STREAM = 1010;
    private static final String IPC_ENDPOINT_FILE = "direct-channel";
    private final boolean ipcEnabled = Boolean.parseBoolean(System.getProperty("kynet.direct.ipc.enabled", "true"));
    private final int maxPeers = Math.max(1, Integer.getInteger("kynet.direct.maxPeers", 256));
    private final long peerBudget = Math.max(1024, Long.getLong("kynet.direct.peerQueueBytes", 4L * 1024 * 1024));
    private final long globalBudget = Math.max(1024, Long.getLong("kynet.direct.queueBytes", 64L * 1024 * 1024));
    private final long sendTimeout = TimeUnit.MILLISECONDS.toNanos(Math.max(1, Long.getLong("kynet.direct.sendTimeoutMs", 5000L)));
    private final long idleTimeout = TimeUnit.MILLISECONDS.toNanos(Math.max(100, Long.getLong("kynet.direct.idleTimeoutMs", 60_000L)));
    private final int termLength = AeronTransport.validatedTermLength(
            Integer.getInteger("kynet.direct.termLength", 8 * 1024 * 1024), "kynet.direct.termLength");
    private final long directOfferRetryNanos = Math.max(0L,
            Long.getLong("kynet.direct.offerRetryNanos", 20_000L));
    private final Map<Integer, Peer> peers = new ConcurrentHashMap<>();
    // Closing an Aeron client attached to a peer's embedded driver can block on that driver's
    // conductor. It must never run on the I/O thread or under the transport lock: either would
    // freeze every send and silently wedge the node. A dedicated closer absorbs those closes.
    private ExecutorService closer;
    private final Map<Integer, Endpoint> replyRoutes = new LinkedHashMap<>();
    private final ThreadLocal<UnsafeBuffer> directSendBuffer =
            ThreadLocal.withInitial(() -> new UnsafeBuffer(ByteBuffer.allocateDirect(1024)));
    private final AtomicLong queued = new AtomicLong();
    private final LongAdder ipcSent = new LongAdder();
    private final LongAdder udpSent = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final LongAdder offerRetries = new LongAdder();
    private final ServiceDirectory directory;
    private final FrameHandler handler;
    private final int instanceId;
    private final String serviceName;
    // Time-ordered so a restart can recognise driver directories left by crashed predecessors.
    private final long incarnation = nextIncarnation();
    private volatile boolean running;
    private volatile Peer cachedPeer;
    private volatile Endpoint cachedSource;
    private long cachedSessionId;
    private int cachedSessionChannelLength;
    private long cachedSessionIncarnation;
    private MediaDriver driver;
    private Aeron aeron;
    private Subscription subscription;
    private Subscription ipcSubscription;
    private ExecutorService worker;
    private Endpoint local;
    private byte[] channelBytes;

    PeerTransport(int instanceId, String serviceName, ServiceDirectory directory, FrameHandler handler) {
        this.instanceId = instanceId;
        this.serviceName = serviceName;
        this.directory = directory;
        this.handler = handler;
    }

    synchronized void start() {
        if (running) return;
        if (closer == null) {
            closer = Executors.newSingleThreadExecutor(task -> {
                Thread thread = new Thread(task, "kynet-direct-closer-" + instanceId);
                thread.setDaemon(true);
                return thread;
            });
        }
        try {
            sweepStaleDriverDirectories();
            String path = directDriverDirectory(instanceId, incarnation).toString();
            driver = MediaDriver.launchEmbedded(new MediaDriver.Context().aeronDirectoryName(path)
                    .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(directThreadingMode()));
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            int port = Integer.getInteger("kynet.direct.port", 0);
            if (port == 0) port = AeronTransport.findAvailablePort();
            if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid direct port");
            String host = System.getProperty("kynet.direct.host", AeronTransport.localHostAddressForReply());
            String channel = AeronTransport.udpChannel(host, port);
            local = new Endpoint(instanceId, incarnation, serviceName, channel, STREAM, Set.of(), Set.of());
            channelBytes = channel.getBytes(StandardCharsets.UTF_8);
            subscription = aeron.addSubscription(AeronTransport.udpChannel(
                    System.getProperty("kynet.direct.bindHost", "0.0.0.0"), port), STREAM);
            if (ipcEnabled) {
                ipcSubscription = aeron.addSubscription(AeronTransport.ipcChannel(), STREAM);
                advertiseIpcEndpoint(path, channel);
            }
            running = true;
            worker = Executors.newSingleThreadExecutor(task -> {
                Thread thread = new Thread(task, "kynet-direct-" + instanceId);
                thread.setDaemon(true);
                return thread;
            });
            worker.execute(this::run);
        } catch (RuntimeException e) { close(); throw e; }
    }

    Endpoint local() { return local; }
    boolean running() { return running; }
    long sentCount() { return ipcSent.sum() + udpSent.sum(); }
    long ipcSentCount() { return ipcSent.sum(); }
    long udpSentCount() { return udpSent.sum(); }
    long droppedCount() { return dropped.sum(); }
    long offerRetryCount() { return offerRetries.sum(); }
    long queuedBytes() { return queued.get(); }

    synchronized Endpoint replyEndpoint(int id) {
        Endpoint endpoint = directory.instance(id);
        return endpoint != null ? endpoint : replyRoutes.get(id);
    }

    int envelopeOverhead() {
        if (!running) throw new IllegalStateException("Direct transport is not running");
        return 40 + channelBytes.length;
    }

    void send(Endpoint target, DirectBuffer frame, int offset, int length) {
        send(target, frame, offset, length, directSendBuffer.get());
    }

    void send(Endpoint target, DirectBuffer frame, int offset, int length, UnsafeBuffer scratch) {
        if (!running) throw new IllegalStateException("Direct transport is not running");
        int total = envelopeOverhead() + length;
        // Same-host IPC offers are thread-safe, so an established peer is written straight
        // into the receiver's log instead of being copied into the queue and re-offered by
        // the single I/O thread. A short spin on BACK_PRESSURED keeps the hot path out of
        // the queue; the queue remains the bounded-memory fallback.
        long retryDeadline = 0L;
        int retryCount = 0;
        for (;;) {
            long result = tryOfferDirect(target, frame, offset, length, total, scratch);
            if (result > 0) {
                return;
            }
            if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED
                    || result == Publication.MAX_POSITION_EXCEEDED) {
                break;
            }
            offerRetries.increment();
            // Check the clock only once per 256 spins: nanoTime dominates the retry loop
            // under sustained backpressure and can consume more than the offer itself.
            if (retryDeadline == 0L) {
                if (directOfferRetryNanos <= 0L) {
                    break;
                }
                retryDeadline = System.nanoTime() + directOfferRetryNanos;
            } else if ((++retryCount & 0xFF) == 0 && System.nanoTime() >= retryDeadline) {
                break;
            }
            Thread.onSpinWait();
        }
        synchronized (this) {
            if (!running) throw new IllegalStateException("Direct transport is closed");
            Peer peer = cachedPeer;
            if (peer != null && (peer.endpoint.instanceId() != target.instanceId()
                    || peer.endpoint.incarnation() != target.incarnation())) {
                peer = null;
            }
            if (peer == null) {
                peer = peers.get(target.instanceId());
            }
            if (peer != null && (peer.publication.isClosed()
                    || !sameEndpoint(peer.endpoint, target))) {
                log.warn("Direct peer unusable, reconnecting instanceId={} publicationClosed={}",
                        peer.endpoint.instanceId(), peer.publication.isClosed());
                discard(peer);
                peers.remove(target.instanceId());
                peer = null;
            }
            if (peer == null) {
                reclaimIdle(System.nanoTime());
                if (peers.size() >= maxPeers) throw new IllegalStateException("Direct peer limit reached");
                peer = connectPeer(target);
                peers.put(target.instanceId(), peer);
            }
            cachedPeer = peer;
            if (total > peer.publication.maxMessageLength()) throw new IllegalArgumentException("Direct frame exceeds maxMessageLength");
            if (peer.bytes + total > peerBudget || queued.get() + total > globalBudget) {
                throw new IllegalStateException("Direct peer is back pressured (peerBytes=" + peer.bytes
                        + ", peerBudget=" + peerBudget + ", queuedBytes=" + queued.get()
                        + ", globalBudget=" + globalBudget + ")");
            }
            byte[] bytes = new byte[total];
            UnsafeBuffer out = new UnsafeBuffer(bytes);
            writeEnvelope(out, peer, frame, offset, length);
            long now = System.nanoTime();
            peer.queue.add(new Outgoing(out, total, now + sendTimeout));
            peer.bytes += total;
            peer.lastUsed = now;
            queued.addAndGet(total);
        }
    }

    /**
     * Sends a frame that the caller already encoded at {@code frameOffset} in {@code buffer},
     * leaving room for the direct envelope in front of it. The envelope prefix is written in
     * place and the combined buffer is offered without copying the business frame.
     */
    void sendPrepared(Endpoint target, UnsafeBuffer buffer, int frameOffset, int frameLength) {
        sendPrepared(target, buffer, frameOffset, frameLength, true);
    }

    /**
     * Same as {@link #sendPrepared}, but the caller has already written the envelope prefix and
     * frame length once via {@link #initializePreparedEnvelope}. This removes a per-message
     * heap-to-direct copy from the hot path of prepared senders.
     */
    void sendPreparedInitialized(Endpoint target, UnsafeBuffer buffer, int frameOffset, int frameLength) {
        sendPrepared(target, buffer, frameOffset, frameLength, false);
    }

    /**
     * Writes the envelope prefix and frame length into a prepared frame buffer. The prefix is
     * constant for a given target instance, so a prepared sender can do this once and then use
     * {@link #sendPreparedInitialized} for every message.
     */
    void initializePreparedEnvelope(Endpoint target, UnsafeBuffer buffer, int frameOffset, int frameLength) {
        if (!running) throw new IllegalStateException("Direct transport is not running");
        int total = frameOffset + frameLength;
        Peer peer = cachedPeer;
        if (peer == null || peer.endpoint.instanceId() != target.instanceId()
                || peer.endpoint.incarnation() != target.incarnation()) {
            peer = resolvePeerForSend(target);
        }
        byte[] prefix = peer.envelopePrefix;
        if (prefix.length != frameOffset) {
            throw new IllegalArgumentException("Envelope offset " + frameOffset
                    + " does not match direct channel prefix " + prefix.length);
        }
        if (total > peer.publication.maxMessageLength()) {
            throw new IllegalArgumentException("Direct frame exceeds maxMessageLength");
        }
        buffer.putBytes(0, prefix, 0, frameOffset);
        buffer.putInt(frameOffset - Integer.BYTES, frameLength);
    }

    private void sendPrepared(Endpoint target, UnsafeBuffer buffer, int frameOffset, int frameLength,
                              boolean writeEnvelope) {
        if (!running) throw new IllegalStateException("Direct transport is not running");
        int total = frameOffset + frameLength;
        Peer peer = cachedPeer;
        if (peer == null || peer.endpoint.instanceId() != target.instanceId()
                || peer.endpoint.incarnation() != target.incarnation()) {
            peer = resolvePeerForSend(target);
        }
        byte[] prefix = peer.envelopePrefix;
        if (prefix.length != frameOffset) {
            throw new IllegalArgumentException("Envelope offset " + frameOffset
                    + " does not match direct channel prefix " + prefix.length);
        }
        // The prepared path validated this once in initializePreparedEnvelope and revalidates
        // whenever the target changes; only the per-message prefix path needs the check here.
        if (writeEnvelope && total > peer.publication.maxMessageLength()) {
            throw new IllegalArgumentException("Direct frame exceeds maxMessageLength");
        }
        long retryDeadline = 0L;
        int retryCount = 0;
        for (;;) {
            if (writeEnvelope) {
                buffer.putBytes(0, prefix, 0, frameOffset);
                buffer.putInt(frameOffset - Integer.BYTES, frameLength);
            }
            long result = peer.publication.offer(buffer, 0, total);
            if (result > 0) {
                if (peer.ipcClient != null) ipcSent.increment();
                else udpSent.increment();
                return;
            }
            if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED
                    || result == Publication.MAX_POSITION_EXCEEDED) {
                break;
            }
            offerRetries.increment();
            // See send(): avoid a nanoTime call on every backpressure spin.
            if (retryDeadline == 0L) {
                if (directOfferRetryNanos <= 0L) {
                    break;
                }
                retryDeadline = System.nanoTime() + directOfferRetryNanos;
            } else if ((++retryCount & 0xFF) == 0 && System.nanoTime() >= retryDeadline) {
                break;
            }
            Thread.onSpinWait();
        }
        synchronized (this) {
            if (!running) throw new IllegalStateException("Direct transport is closed");
            // The peer was resolved before the offer attempt; the closer may have reclaimed it in
            // the meantime. Queueing to a removed peer would black-hole the message, so re-resolve.
            if (peers.get(peer.endpoint.instanceId()) != peer) {
                peer = resolvePeerForSend(target);
            }
            if (peer.bytes + total > peerBudget || queued.get() + total > globalBudget) {
                throw new IllegalStateException("Direct peer is back pressured (peerBytes=" + peer.bytes
                        + ", peerBudget=" + peerBudget + ", queuedBytes=" + queued.get()
                        + ", globalBudget=" + globalBudget + ")");
            }
            byte[] bytes = new byte[total];
            buffer.getBytes(0, bytes, 0, total);
            long now = System.nanoTime();
            peer.queue.add(new Outgoing(new UnsafeBuffer(bytes), total, now + sendTimeout));
            peer.bytes += total;
            peer.lastUsed = now;
            queued.addAndGet(total);
        }
    }

    private Peer resolvePeerForSend(Endpoint target) {
        synchronized (this) {
            if (!running) throw new IllegalStateException("Direct transport is closed");
            Peer peer = peers.get(target.instanceId());
            if (peer != null && (peer.publication.isClosed() || !sameEndpoint(peer.endpoint, target))) {
                log.warn("Direct peer unusable, reconnecting instanceId={} publicationClosed={}",
                        peer.endpoint.instanceId(), peer.publication.isClosed());
                discard(peer);
                peers.remove(target.instanceId());
                peer = null;
            }
            if (peer == null) {
                reclaimIdle(System.nanoTime());
                if (peers.size() >= maxPeers) throw new IllegalStateException("Direct peer limit reached");
                peer = connectPeer(target);
                peers.put(target.instanceId(), peer);
            }
            cachedPeer = peer;
            return peer;
        }
    }

    private long tryOfferDirect(Endpoint target, DirectBuffer frame, int offset, int length, int total,
                                UnsafeBuffer out) {
        // The hot path targets one peer; a single-entry cache avoids a boxing map
        // lookup on every frame. Falls back to the map on incarnation changes.
        Peer peer = cachedPeer;
        if (peer == null || peer.endpoint.instanceId() != target.instanceId()
                || peer.endpoint.incarnation() != target.incarnation()) {
            peer = peers.get(target.instanceId());
            if (peer == null || !sameEndpoint(peer.endpoint, target)) {
                return Publication.NOT_CONNECTED;
            }
            cachedPeer = peer;
        }
        Publication publication = peer.publication;
        if (total > publication.maxMessageLength()) {
            throw new IllegalArgumentException("Direct frame exceeds maxMessageLength");
        }
        if (out.capacity() < total) {
            out = new UnsafeBuffer(ByteBuffer.allocateDirect(Math.max(total, out.capacity() * 2)));
            directSendBuffer.set(out);
        }
        writeEnvelope(out, peer, frame, offset, length);
        peer.lastUsed = System.nanoTime();
        long result = publication.offer(out, 0, total);
        if (result > 0) {
            if (peer.ipcClient != null) ipcSent.increment();
            else udpSent.increment();
        }
        return result;
    }

    private void writeEnvelope(UnsafeBuffer out, Peer peer, DirectBuffer frame, int offset, int length) {
        byte[] prefix = peer.envelopePrefix;
        out.putBytes(0, prefix, 0, prefix.length);
        out.putInt(prefix.length - Integer.BYTES, length);
        out.putBytes(prefix.length, frame, offset, length);
    }

    private void run() {
        FragmentAssembler assembler = new FragmentAssembler(this::receive);
        FragmentAssembler ipcAssembler = new FragmentAssembler(this::receive);
        // The common case is a message that fits in one fragment. Skip the assembler for it
        // and only pay for fragment reassembly when Aeron actually reports a fragmented frame.
        FragmentHandler udpHandler = unfragmentedFastPath(assembler);
        FragmentHandler ipcHandler = unfragmentedFastPath(ipcAssembler);
        IdleStrategy idle = directIdleStrategy();
        int pollLimit = Math.max(1, Integer.getInteger("kynet.direct.pollFragmentLimit", 1024));
        int idleLoops = 0;
        while (running) {
            try {
                int work = subscription.poll(udpHandler, pollLimit);
                if (ipcSubscription != null) {
                    work += ipcSubscription.poll(ipcHandler, pollLimit);
                }
                if (queued.get() == 0L) {
                    // Direct offers from sender threads keep the queue empty on the fast path;
                    // only reclaim idle peers periodically.
                    if (work == 0 && (++idleLoops & 0x3FF) == 0) {
                        reclaimIdle(System.nanoTime());
                    }
                } else {
                    synchronized (this) {
                        long now = System.nanoTime();
                        for (Peer peer : peers.values()) {
                            for (int i = 0; i < 64 && !peer.queue.isEmpty(); i++) {
                                Outgoing message = peer.queue.peek();
                                long result = peer.publication.isClosed() ? Publication.CLOSED
                                        : peer.publication.offer(message.buffer, 0, message.length);
                                if (result > 0 || now >= message.deadline || result == Publication.CLOSED
                                        || result == Publication.MAX_POSITION_EXCEEDED) {
                                    peer.queue.remove();
                                    peer.bytes -= message.length;
                                    queued.addAndGet(-message.length);
                                    if (result > 0) {
                                        if (peer.ipcClient != null) ipcSent.increment(); else udpSent.increment();
                                    } else {
                                        dropped.increment();
                                        log.warn("Direct send dropped result={} instanceId={} incarnation={} "
                                                        + "queueSize={} publicationClosed={}",
                                                result == Publication.CLOSED ? "CLOSED" : result,
                                                peer.endpoint.instanceId(), peer.endpoint.incarnation(),
                                                peer.queue.size(), peer.publication.isClosed());
                                    }
                                    work++;
                                } else {
                                    break;
                                }
                            }
                        }
                        reclaimIdle(now);
                    }
                }
                idle.idle(work);
            } catch (RuntimeException e) {
                if (running) log.warn("Direct I/O error: {}", e.toString());
            }
        }
    }

    private FragmentHandler unfragmentedFastPath(FragmentAssembler assembler) {
        return (buffer, offset, length, header) -> {
            // UNFRAGMENTED is 0xC0 (both high bits); BEGIN_FRAGMENT is 0x40, so a bitwise
            // "any bit" test would misclassify the first fragment of a large message.
            if (header.flags() == FrameDescriptor.UNFRAGMENTED) {
                receive(buffer, offset, length, header);
            } else {
                assembler.onFragment(buffer, offset, length, header);
            }
        };
    }

    private void receive(DirectBuffer buffer, int offset, int length, io.aeron.logbuffer.Header header) {
        try {
            // The envelope identity (source instance/incarnation, target, reply channel) is
            // fixed for an Aeron session. Validate it once per session, then skip the repeated
            // envelope/header checks for every following message from the same publication.
            long sessionId = header.sessionId();
            if (length >= 40 && cachedSessionId != 0L && sessionId == cachedSessionId) {
                int frameOffset = offset + 40 + cachedSessionChannelLength;
                int frameLength = length - 40 - cachedSessionChannelLength;
                if (frameLength >= FrameCodec.FIXED_HEADER_LENGTH + 14) {
                    handler.onFrame(buffer, frameOffset, frameLength, cachedSessionIncarnation);
                    return;
                }
            }
            if (length < 40 || buffer.getInt(offset) != MAGIC || buffer.getInt(offset + 16) != instanceId
                    || buffer.getLong(offset + 20) != incarnation) {
                dropped.increment();
                if (length >= 40 && buffer.getInt(offset) == MAGIC) {
                    log.warn("Direct envelope rejected targetInstance={} targetIncarnation={} expected={} actual={} sourceInstance={}",
                            buffer.getInt(offset + 16), buffer.getLong(offset + 20), instanceId, incarnation,
                            buffer.getInt(offset + 4));
                }
                return;
            }
            int source = buffer.getInt(offset + 4);
            long sourceIncarnation = buffer.getLong(offset + 8);
            int stream = buffer.getInt(offset + 28), channelLength = buffer.getInt(offset + 32);
            if (channelLength < 1 || channelLength > 1024 || channelLength > length - 40) throw new IllegalArgumentException("Invalid reply endpoint");
            int frameLength = buffer.getInt(offset + 36 + channelLength);
            int frameOffset = offset + 40 + channelLength;
            if (frameLength < FrameCodec.FIXED_HEADER_LENGTH + 14 || frameLength != length - 40 - channelLength
                    || FrameCodec.readSourceInstanceId(buffer, frameOffset) != source) throw new IllegalArgumentException("Invalid direct frame");
            int frameTarget = FrameCodec.readTargetInstanceId(buffer, frameOffset);
            if (frameTarget != 0 && frameTarget != instanceId) throw new IllegalArgumentException("Wrong direct target");
            Endpoint registered = cachedSource;
            if (registered == null || registered.instanceId() != source
                    || registered.incarnation() != sourceIncarnation) {
                registered = directory.instance(source);
                if (registered != null) {
                    cachedSource = registered;
                }
            }
            if (registered != null) {
                // Registered instances already have a discovery endpoint, so the reply channel
                // carried by every envelope does not need decoding on the hot receive path.
                if (registered.incarnation() != sourceIncarnation) { dropped.increment(); return; }
                cachedSessionId = sessionId;
                cachedSessionChannelLength = channelLength;
                cachedSessionIncarnation = sourceIncarnation;
                handler.onFrame(buffer, frameOffset, frameLength, sourceIncarnation);
                return;
            }
            byte[] channel = new byte[channelLength];
            buffer.getBytes(offset + 36, channel);
            Endpoint reply = new Endpoint(source, sourceIncarnation, "reply",
                    new String(channel, StandardCharsets.UTF_8), stream, Set.of(), Set.of());
            // Reply-only addresses do not become discovery candidates. Service traffic assumes a trusted network.
            synchronized (this) {
                if (!replyRoutes.containsKey(source) && replyRoutes.size() >= maxPeers) {
                    replyRoutes.remove(replyRoutes.keySet().iterator().next());
                }
                replyRoutes.put(source, reply);
            }
            handler.onFrame(buffer, frameOffset, frameLength, sourceIncarnation);
        } catch (RuntimeException e) {
            dropped.increment();
            log.debug("Rejected direct message: {}", e.toString());
        }
    }

    static boolean sameEndpoint(Endpoint a, Endpoint b) {
        return a.instanceId() == b.instanceId() && a.incarnation() == b.incarnation()
                && a.streamId() == b.streamId() && a.channel().equals(b.channel());
    }

    /**
     * Shared mode drives the media driver on the application's offer/poll threads and is the
     * default. Dedicated mode gives the driver its own conductor/sender/receiver threads; the
     * raw Aeron microbenchmark showed a gain, but the direct-transport term length dominates
     * end-to-end throughput, so measure before changing this.
     */
    /**
     * Yield is the low-latency default: a parked poll thread wakes on the OS timer, which added
     * milliseconds to p99 in the RPC latency benchmark (especially on Windows). Use "busy" for
     * the lowest tail and "backoff" when CPU is more important than latency.
     */
    private static IdleStrategy directIdleStrategy() {
        String configured = System.getProperty("kynet.direct.idleStrategy", "yield");
        return switch (configured.trim().toLowerCase(Locale.ROOT)) {
            case "backoff" -> new BackoffIdleStrategy(1, 10, 1, 100_000);
            case "yield" -> new YieldingIdleStrategy();
            case "busy" -> new BusySpinIdleStrategy();
            case "noop" -> new NoOpIdleStrategy();
            default -> throw new IllegalArgumentException("Invalid kynet.direct.idleStrategy: "
                    + configured + " (expected backoff, yield, busy or noop)");
        };
    }

    private static ThreadingMode directThreadingMode() {
        String configured = System.getProperty("kynet.direct.threadingMode", "SHARED");
        try {
            return ThreadingMode.valueOf(configured.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid kynet.direct.threadingMode: " + configured
                    + " (expected SHARED, DEDICATED or INVOKER)", e);
        }
    }

    private static Path directDriverDirectory(int id, long generation) {
        return Path.of(AeronTransport.aeronBaseDirectory(),
                "kynet-direct-" + id + "-" + Long.toUnsignedString(generation)).toAbsolutePath();
    }

    private void advertiseIpcEndpoint(String path, String channel) {
        try {
            Files.writeString(Path.of(path, IPC_ENDPOINT_FILE), channel, StandardCharsets.UTF_8);
        } catch (IOException | SecurityException e) {
            log.warn("Cannot advertise direct IPC endpoint; peers will use UDP: {}", e.toString());
        }
    }

    private Peer connectPeer(Endpoint target) {
        if (ipcEnabled && hasLocalIpcEndpoint(target)) {
            Aeron ipcClient = null;
            try {
                String path = directDriverDirectory(target.instanceId(), target.incarnation()).toString();
                // IPC publications must use the receiver's driver, not this client's independent driver.
                // The conductor's errorHandler must stay visible: a stopped conductor gets its
                // publications reaped by the peer driver (client liveness timeout) and every offer
                // afterwards returns CLOSED, which used to wedge the send path silently.
                ipcClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(path)
                        .driverTimeoutMs(Long.getLong("kynet.direct.driverTimeoutMs", 10_000L))
                        .errorHandler(error -> log.warn("Direct IPC driver error instanceId={}: {}",
                                target.instanceId(), error.toString())));
                Publication publication = ipcClient.addPublication(
                        AeronTransport.ipcChannel() + "?term-length=" + termLength, target.streamId());
                log.info("Direct peer connected instanceId={} transport=IPC driverDir={}", target.instanceId(), path);
                return new Peer(target, publication, ipcClient, createEnvelopePrefix(target));
            } catch (RuntimeException e) {
                org.agrona.CloseHelper.quietClose(ipcClient);
                log.info("Direct IPC unavailable instanceId={}; using UDP: {}", target.instanceId(), e.toString());
            }
        }
        Publication publication = aeron.addPublication(target.channel() + "|term-length=" + termLength, target.streamId());
        log.info("Direct peer connected instanceId={} transport=UDP channel={}", target.instanceId(), target.channel());
        return new Peer(target, publication, null, createEnvelopePrefix(target));
    }

    private byte[] createEnvelopePrefix(Endpoint target) {
        byte[] prefix = new byte[envelopeOverhead()];
        UnsafeBuffer out = new UnsafeBuffer(prefix);
        out.putInt(0, MAGIC);
        out.putInt(4, instanceId);
        out.putLong(8, incarnation);
        out.putInt(16, target.instanceId());
        out.putLong(20, target.incarnation());
        out.putInt(28, STREAM);
        out.putInt(32, channelBytes.length);
        out.putBytes(36, channelBytes);
        return prefix;
    }

    private boolean hasLocalIpcEndpoint(Endpoint target) {
        try {
            Path marker = directDriverDirectory(target.instanceId(), target.incarnation()).resolve(IPC_ENDPOINT_FILE);
            if (!Files.isRegularFile(marker) || Files.size(marker) > 1024
                    || !target.channel().equals(Files.readString(marker, StandardCharsets.UTF_8))) return false;
            String endpoint = ChannelUri.parse(target.channel()).get(CommonContext.ENDPOINT_PARAM_NAME);
            String host = endpoint.substring(0, endpoint.lastIndexOf(':'));
            for (InetAddress address : InetAddress.getAllByName(host)) {
                if (address.isLoopbackAddress() || NetworkInterface.getByInetAddress(address) != null) return true;
            }
        } catch (IOException | IllegalArgumentException | SecurityException e) {
            log.debug("Cannot access direct IPC endpoint instanceId={}: {}", target.instanceId(), e.toString());
        }
        return false;
    }

    private synchronized void reclaimIdle(long now) {
        for (Peer peer : peers.values()) {
            if (peer.queue.isEmpty() && now - peer.lastUsed >= idleTimeout
                    && peers.remove(peer.endpoint.instanceId(), peer)) {
                if (cachedPeer == peer) {
                    cachedPeer = null;
                }
                log.info("Direct peer reclaimed after idle instanceId={} incarnation={} droppedTotal={}",
                        peer.endpoint.instanceId(), peer.endpoint.incarnation(), dropped.sum());
                closePeerAsync(peer);
            }
        }
    }

    private void discard(Peer peer) {
        if (cachedPeer == peer) {
            cachedPeer = null;
        }
        Outgoing message;
        while ((message = peer.queue.poll()) != null) {
            peer.bytes -= message.length;
            queued.addAndGet(-message.length);
            dropped.increment();
        }
        closePeerAsync(peer);
    }

    private void closePeerAsync(Peer peer) {
        ExecutorService pool = closer;
        if (pool == null) {
            closePeer(peer);
            return;
        }
        try {
            pool.execute(() -> closePeer(peer));
        } catch (RejectedExecutionException e) {
            closePeer(peer);
        }
    }

    private static void closePeer(Peer peer) {
        org.agrona.CloseHelper.quietClose(peer.publication);
        org.agrona.CloseHelper.quietClose(peer.ipcClient);
    }

    @Override public void close() {
        running = false;
        if (worker != null) {
            worker.shutdown();
            try { worker.awaitTermination(3, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        synchronized (this) {
            for (Peer peer : peers.values()) discard(peer);
            peers.clear();
            replyRoutes.clear();
            if (subscription != null) subscription.close();
            if (ipcSubscription != null) ipcSubscription.close();
            if (aeron != null) aeron.close();
            if (driver != null) driver.close();
            subscription = null; ipcSubscription = null; aeron = null; driver = null;
        }
        if (closer != null) {
            closer.shutdown();
            try { closer.awaitTermination(1, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /**
     * Millis in the high bits with 20 random low bits, so restarts get larger incarnations (the
     * stale-directory sweep treats a smaller incarnation as "crashed predecessor") while two
     * starts within the same millisecond still collide on a directory name with probability 2^-20.
     */
    private static long nextIncarnation() {
        long value = (System.currentTimeMillis() << 20) | ThreadLocalRandom.current().nextInt(1 << 20);
        return value != 0 ? value : 1;
    }

    /**
     * Deletes driver directories from crashed predecessors of this same instanceId. Clean exits
     * remove their own directory via dirDeleteOnShutdown, but a crash leaks the ~47MB CnC file on
     * every restart, and a fresh incarnation would otherwise never match the old directory name.
     * Restricting the sweep to this instanceId is deliberate: another live process on this machine
     * has a different instanceId, and a duplicated instanceId is a registration error already.
     * Best effort — on Windows mapped files of a not-yet-reaped process may fail to delete.
     */
    private void sweepStaleDriverDirectories() {
        try {
            String prefix = "kynet-direct-" + instanceId + "-";
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(Path.of(AeronTransport.aeronBaseDirectory()))) {
                for (Path entry : entries) {
                    String name = entry.getFileName().toString();
                    if (!name.startsWith(prefix)) continue;
                    long staleIncarnation;
                    try {
                        staleIncarnation = Long.parseUnsignedLong(name.substring(prefix.length()));
                    } catch (NumberFormatException notOurs) {
                        continue;
                    }
                    if (Long.compareUnsigned(staleIncarnation, incarnation) >= 0) continue;
                    org.agrona.IoUtil.delete(entry.toFile(), true);
                    log.debug("Deleted stale direct driver directory {}", name);
                }
            }
        } catch (IOException | RuntimeException e) {
            log.debug("Cannot sweep stale direct driver directories instanceId={}: {}", instanceId, e.toString());
        }
    }

    private record Outgoing(UnsafeBuffer buffer, int length, long deadline) {}
    @FunctionalInterface interface FrameHandler {
        void onFrame(DirectBuffer buffer, int offset, int length, long sourceIncarnation);
    }
    private static final class Peer {
        final Endpoint endpoint;
        final Publication publication;
        final Aeron ipcClient;
        final byte[] envelopePrefix;
        final ArrayDeque<Outgoing> queue = new ArrayDeque<>();
        long bytes;
        volatile long lastUsed = System.nanoTime();
        Peer(Endpoint endpoint, Publication publication, Aeron ipcClient, byte[] envelopePrefix) {
            this.endpoint = endpoint;
            this.publication = publication;
            this.ipcClient = ipcClient;
            this.envelopePrefix = envelopePrefix;
        }
    }
}
