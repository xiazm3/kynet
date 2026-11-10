package cn.itsub.kynet.common.protocol;

import java.io.*;
import java.util.*;

/** Bounded, versioned full snapshots. A client commits only a complete snapshot. */
public final class ServiceDirectoryCodec {
    public static final int VERSION = 1;
    public static final int CHUNK_BYTES = 48 * 1024;
    private static final int MAX_ENTRIES = 100_000;

    private ServiceDirectoryCodec() {}

    public record Endpoint(int instanceId, long incarnation, String serviceName,
                           String channel, int streamId, Set<Integer> actions, Set<String> topics) {
        public Endpoint {
            if (instanceId == 0 || incarnation == 0 || serviceName == null || serviceName.isBlank()
                    || channel == null || !channel.startsWith("aeron:udp?endpoint=") || streamId <= 0) {
                throw new IllegalArgumentException("Invalid direct endpoint");
            }
            actions = Set.copyOf(actions);
            topics = Set.copyOf(topics);
        }
    }

    public record Chunk(long revision, int index, int count, List<Endpoint> endpoints) {}

    public static List<byte[]> encode(long revision, Collection<Endpoint> endpoints) {
        if (revision < 0 || endpoints.size() > MAX_ENTRIES) throw new IllegalArgumentException("Directory limit exceeded");
        try {
            List<byte[]> records = new ArrayList<>();
            for (Endpoint endpoint : endpoints) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes);
                out.writeInt(endpoint.instanceId());
                out.writeLong(endpoint.incarnation());
                out.writeUTF(endpoint.serviceName());
                out.writeUTF(endpoint.channel());
                out.writeInt(endpoint.streamId());
                out.writeInt(endpoint.actions().size());
                for (int action : new TreeSet<>(endpoint.actions())) out.writeInt(action);
                out.writeInt(endpoint.topics().size());
                for (String topic : new TreeSet<>(endpoint.topics())) out.writeUTF(topic);
                if (bytes.size() > CHUNK_BYTES - 24) throw new IllegalArgumentException("Endpoint metadata too large");
                records.add(bytes.toByteArray());
            }
            List<List<byte[]>> batches = new ArrayList<>();
            List<byte[]> batch = new ArrayList<>();
            int size = 24;
            for (byte[] record : records) {
                if (size + record.length > CHUNK_BYTES) {
                    batches.add(batch);
                    batch = new ArrayList<>();
                    size = 24;
                }
                batch.add(record);
                size += record.length;
            }
            batches.add(batch);
            if (batches.size() > 4096) throw new IllegalArgumentException("Directory chunk limit exceeded");
            List<byte[]> result = new ArrayList<>();
            for (int i = 0; i < batches.size(); i++) {
                ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes);
                out.writeInt(VERSION);
                out.writeLong(revision);
                out.writeInt(i);
                out.writeInt(batches.size());
                out.writeInt(batches.get(i).size());
                for (byte[] record : batches.get(i)) out.write(record);
                result.add(bytes.toByteArray());
            }
            return result;
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot encode directory", e);
        }
    }

    public static Chunk decode(byte[] bytes) {
        if (bytes.length > CHUNK_BYTES) throw new IllegalArgumentException("Directory chunk too large");
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            if (in.readInt() != VERSION) throw new IOException("Unsupported directory version");
            long revision = in.readLong();
            int index = in.readInt(), count = in.readInt(), entries = in.readInt();
            if (revision < 0 || count < 1 || count > 4096 || index < 0 || index >= count
                    || entries < 0 || entries > MAX_ENTRIES) throw new IOException("Invalid directory chunk");
            List<Endpoint> endpoints = new ArrayList<>();
            for (int i = 0; i < entries; i++) {
                int id = in.readInt();
                long incarnation = in.readLong();
                String service = in.readUTF(), channel = in.readUTF();
                int stream = in.readInt();
                Set<Integer> actions = new HashSet<>();
                int actionCount = boundedCount(in);
                for (int j = 0; j < actionCount; j++) actions.add(in.readInt());
                Set<String> topics = new HashSet<>();
                int topicCount = boundedCount(in);
                for (int j = 0; j < topicCount; j++) topics.add(in.readUTF());
                endpoints.add(new Endpoint(id, incarnation, service, channel, stream, actions, topics));
            }
            if (in.available() != 0) throw new IOException("Trailing directory bytes");
            return new Chunk(revision, index, count, List.copyOf(endpoints));
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid directory", e);
        }
    }

    private static int boundedCount(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > MAX_ENTRIES) throw new IOException("Invalid directory count");
        return count;
    }
}
