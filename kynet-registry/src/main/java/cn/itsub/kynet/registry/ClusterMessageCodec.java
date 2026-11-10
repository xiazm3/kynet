package cn.itsub.kynet.registry;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class ClusterMessageCodec {
    static final int MAX_DATAGRAM_BYTES = 60 * 1024;
    static final int MAX_MEMBERS = 256;
    static final int MAX_ROUTES_PER_CHUNK = 1_024;
    static final int MAX_GOSSIP_CHUNKS = 256;

    private static final int MAGIC = 0x4B59434C;
    private static final byte PROTOCOL_VERSION = 4;
    private static final byte TYPE_DISCOVERY = 1;
    private static final byte TYPE_GOSSIP = 2;
    private static final int MAC_BYTES = 32;
    private ClusterMessageCodec() {
    }

    static byte[] encodeDiscovery(RegistryNodeConfig config, RegistryMember self) {
        return encodeSigned(config, TYPE_DISCOVERY, out -> {
            out.writeUTF(config.clusterHost());
            out.writeInt(config.clientPort());
            out.writeInt(config.clusterPort());
            out.writeLong(self == null ? 1 : self.incarnation());
            out.writeLong(self == null ? 1 : self.version());
            out.writeUTF(config.clientHost());
        });
    }

    static byte[] encodeGossip(RegistryNodeConfig config, List<RegistryMember> members,
                               List<ClusterRoute> routes, long routeSnapshotVersion,
                               int chunkIndex, int chunkCount) {
        if (members == null || members.size() > MAX_MEMBERS) {
            throw new IllegalArgumentException("Broker member count exceeds " + MAX_MEMBERS);
        }
        if (routes == null || routes.size() > MAX_ROUTES_PER_CHUNK) {
            throw new IllegalArgumentException("Broker route chunk exceeds " + MAX_ROUTES_PER_CHUNK);
        }
        if (chunkIndex < 0 || chunkCount <= 0 || chunkCount > MAX_GOSSIP_CHUNKS
                || chunkIndex >= chunkCount) {
            throw new IllegalArgumentException("Invalid gossip chunk " + chunkIndex + "/" + chunkCount);
        }
        return encodeSigned(config, TYPE_GOSSIP, out -> {
            out.writeLong(routeSnapshotVersion);
            out.writeInt(chunkIndex);
            out.writeInt(chunkCount);
            out.writeInt(members.size());
            for (RegistryMember member : members) {
                writeMember(out, member);
            }
            out.writeInt(routes.size());
            for (ClusterRoute route : routes) {
                writeRoute(out, route);
            }
        });
    }

    static ClusterMessage decode(RegistryNodeConfig config, byte[] payload, int length) {
        return decode(config, payload, length, System.currentTimeMillis());
    }

    static ClusterMessage decode(RegistryNodeConfig config, byte[] payload, int length, long nowMillis) {
        if (payload == null || length <= MAC_BYTES || length > payload.length
                || length > MAX_DATAGRAM_BYTES) {
            return null;
        }
        int bodyLength = length - MAC_BYTES;
        if (!verifyMac(config, payload, bodyLength, payload, bodyLength)) {
            return null;
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload, 0, bodyLength));
            if (in.readInt() != MAGIC || in.readByte() != PROTOCOL_VERSION) {
                return null;
            }
            byte type = in.readByte();
            long sentAtMillis = in.readLong();
            long nonce = in.readLong();
            long skew = config.protocolClockSkewMillis();
            if (sentAtMillis < nowMillis - skew || sentAtMillis > nowMillis + skew) {
                return null;
            }
            String clusterName = in.readUTF();
            String fromNodeId = in.readUTF();
            long fromEpoch = in.readLong();
            return switch (type) {
                case TYPE_DISCOVERY -> decodeDiscovery(in, clusterName, fromNodeId, fromEpoch, nonce);
                case TYPE_GOSSIP -> decodeGossip(in, clusterName, fromNodeId, fromEpoch, nonce);
                default -> null;
            };
        } catch (IOException | IllegalArgumentException e) {
            return null;
        }
    }

    private static DiscoveryMessage decodeDiscovery(DataInputStream in, String clusterName,
                                                     String fromNodeId, long fromEpoch,
                                                     long nonce) throws IOException {
        String clusterHost = in.readUTF();
        int clientPort = in.readInt();
        int clusterPort = in.readInt();
        long incarnation = in.readLong();
        long version = in.readLong();
        String clientHost = in.readUTF();
        if (in.available() != 0) {
            throw new IOException("Unexpected trailing discovery bytes");
        }
        return new DiscoveryMessage(clusterName, fromNodeId, fromEpoch, clusterHost, clientHost,
                clientPort, clusterPort, incarnation, version, nonce);
    }

    private static GossipMessage decodeGossip(DataInputStream in, String clusterName,
                                              String fromNodeId, long fromEpoch,
                                              long nonce) throws IOException {
        long routeSnapshotVersion = in.readLong();
        int chunkIndex = in.readInt();
        int chunkCount = in.readInt();
        if (chunkIndex < 0 || chunkCount <= 0 || chunkIndex >= chunkCount
                || chunkCount > MAX_GOSSIP_CHUNKS) {
            throw new IOException("Invalid gossip chunk metadata");
        }

        int memberCount = in.readInt();
        if (memberCount < 0 || memberCount > MAX_MEMBERS) {
            throw new IOException("Invalid broker member count " + memberCount);
        }
        List<RegistryMember> members = new ArrayList<>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            members.add(readMember(in));
        }

        int routeCount = in.readInt();
        if (routeCount < 0 || routeCount > MAX_ROUTES_PER_CHUNK) {
            throw new IOException("Invalid broker route count " + routeCount);
        }
        List<ClusterRoute> routes = new ArrayList<>(routeCount);
        for (int i = 0; i < routeCount; i++) {
            routes.add(readRoute(in));
        }
        if (in.available() != 0) {
            throw new IOException("Unexpected trailing gossip bytes");
        }
        return new GossipMessage(clusterName, fromNodeId, fromEpoch,
                routeSnapshotVersion, chunkIndex, chunkCount, members, routes, nonce);
    }

    private static byte[] encodeSigned(RegistryNodeConfig config, byte type, MessageWriter writer) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeByte(PROTOCOL_VERSION);
            out.writeByte(type);
            out.writeLong(System.currentTimeMillis());
            out.writeLong(config.nextProtocolNonce());
            out.writeUTF(config.clusterName());
            out.writeUTF(config.nodeId());
            out.writeLong(config.epoch());
            writer.write(out);
            out.flush();
            byte[] body = bytes.toByteArray();
            byte[] mac = config.computeProtocolMac(body, body.length);
            long maxMessageBytes = MAX_DATAGRAM_BYTES;
            if ((long) body.length + mac.length > maxMessageBytes) {
                throw new IllegalArgumentException("Cluster message exceeds " + maxMessageBytes + " bytes");
            }
            byte[] signed = Arrays.copyOf(body, body.length + mac.length);
            System.arraycopy(mac, 0, signed, body.length, mac.length);
            return signed;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode broker cluster message", e);
        }
    }

    private static void writeMember(DataOutputStream out, RegistryMember member) throws IOException {
        out.writeUTF(member.nodeId());
        out.writeLong(member.epoch());
        out.writeUTF(member.host());
        out.writeUTF(member.clientHost());
        out.writeInt(member.clientPort());
        out.writeInt(member.clusterPort());
        out.writeLong(member.incarnation());
        out.writeUTF(member.state().name());
        out.writeLong(member.version());
    }

    private static RegistryMember readMember(DataInputStream in) throws IOException {
        return new RegistryMember(
                in.readUTF(),
                in.readLong(),
                in.readUTF(),
                in.readUTF(),
                in.readInt(),
                in.readInt(),
                in.readLong(),
                MemberState.valueOf(in.readUTF()),
                in.readLong(),
                0L);
    }

    static int encodedRouteSize(ClusterRoute route) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            writeRoute(new DataOutputStream(bytes), route);
            return bytes.size();
        } catch (IOException e) { throw new IllegalArgumentException("Invalid route", e); }
    }

    private static void writeRoute(DataOutputStream out, ClusterRoute route) throws IOException {
        out.writeUTF(route.key());
        out.writeUTF(route.kind().name());
        out.writeUTF(route.ownerNodeId());
        out.writeLong(route.ownerEpoch());
        out.writeInt(route.instanceId());
        out.writeLong(route.instanceGeneration());
        out.writeUTF(route.serviceName() == null ? "" : route.serviceName());
        out.writeInt(route.serviceId());
        out.writeInt(route.action());
        out.writeLong(route.version());
        out.writeUTF(route.endpointChannel());
        out.writeInt(route.endpointStreamId());
        out.writeLong(route.incarnation());
        out.writeUTF(route.topicName());
    }

    private static ClusterRoute readRoute(DataInputStream in) throws IOException {
        return new ClusterRoute(
                in.readUTF(),
                RouteKind.valueOf(in.readUTF()),
                in.readUTF(),
                in.readLong(),
                in.readInt(),
                in.readLong(),
                emptyToNull(in.readUTF()),
                in.readInt(),
                in.readInt(),
                in.readLong(),
                in.readUTF(),
                in.readInt(),
                in.readLong(),
                in.readUTF());
    }

    private static boolean verifyMac(RegistryNodeConfig config, byte[] payload, int length,
                                     byte[] expectedPayload, int expectedOffset) {
        byte[] actual = config.computeProtocolMac(payload, length);
        byte[] expected = Arrays.copyOfRange(expectedPayload, expectedOffset, expectedOffset + MAC_BYTES);
        return MessageDigest.isEqual(actual, expected);
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    @FunctionalInterface
    private interface MessageWriter {
        void write(DataOutputStream out) throws IOException;
    }

    sealed interface ClusterMessage permits DiscoveryMessage, GossipMessage {
        String clusterName();

        String fromNodeId();

        long fromEpoch();

        long nonce();
    }

    record DiscoveryMessage(String clusterName, String fromNodeId, long fromEpoch,
                            String host, String clientHost, int clientPort, int clusterPort,
                            long incarnation, long version, long nonce) implements ClusterMessage {
        DiscoveryMessage(String clusterName, String fromNodeId, long fromEpoch, String host,
                         int clientPort, int clusterPort, long incarnation, long version) {
            this(clusterName, fromNodeId, fromEpoch, host, host, clientPort, clusterPort, incarnation, version, 0L);
        }
    }

    record GossipMessage(String clusterName, String fromNodeId, long fromEpoch,
                         long routeSnapshotVersion, int chunkIndex, int chunkCount,
                         List<RegistryMember> members, List<ClusterRoute> routes, long nonce)
            implements ClusterMessage {
    }


}
