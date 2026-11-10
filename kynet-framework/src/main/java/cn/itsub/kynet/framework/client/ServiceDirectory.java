package cn.itsub.kynet.framework.client;

import cn.itsub.kynet.common.protocol.ServiceDirectoryCodec;
import cn.itsub.kynet.common.protocol.ServiceDirectoryCodec.Endpoint;
import org.agrona.collections.Int2ObjectHashMap;
import java.util.*;

final class ServiceDirectory {
    private volatile View view = new View(new Int2ObjectHashMap<>(), new Int2ObjectHashMap<>(),
            Map.of(), Map.of());
    private volatile long revision = -1;
    private volatile long version;
    private long assembling = -1;
    private long registrationSession;
    private List<Endpoint>[] chunks;

    synchronized void reconnect() {
        reconnect(0L);
    }

    synchronized void reconnect(long session) {
        registrationSession = session;
        revision = -1;
        assembling = -1;
        chunks = null;
        version++;
    }

    synchronized boolean accept(ServiceDirectoryCodec.Chunk chunk) {
        return accept(chunk, 0L);
    }

    @SuppressWarnings("unchecked")
    synchronized boolean accept(ServiceDirectoryCodec.Chunk chunk, long session) {
        if (session != registrationSession) return false;
        if (chunk.revision() <= revision || chunk.revision() < assembling) return false;
        if (chunk.revision() != assembling) {
            assembling = chunk.revision();
            chunks = new List[chunk.count()];
        }
        if (chunks.length != chunk.count()) throw new IllegalArgumentException("Directory chunk count changed");
        chunks[chunk.index()] = chunk.endpoints();
        Int2ObjectHashMap<Endpoint> next = new Int2ObjectHashMap<>();
        for (List<Endpoint> part : chunks) {
            if (part == null) return false;
            for (Endpoint endpoint : part) {
                if (next.put(endpoint.instanceId(), endpoint) != null) {
                    throw new IllegalArgumentException("Duplicate directory identity");
                }
                if (next.size() > 100_000) throw new IllegalArgumentException("Directory too large");
            }
        }
        Int2ObjectHashMap<List<Endpoint>> actions = new Int2ObjectHashMap<>();
        Map<String, List<Endpoint>> services = new HashMap<>(), topics = new HashMap<>();
        for (Endpoint endpoint : next.values()) {
            services.computeIfAbsent(endpoint.serviceName(), key -> new ArrayList<>()).add(endpoint);
            for (int action : endpoint.actions()) actions.computeIfAbsent(action, key -> new ArrayList<>()).add(endpoint);
            for (String topic : endpoint.topics()) topics.computeIfAbsent(topic, key -> new ArrayList<>()).add(endpoint);
        }
        actions.replaceAll((action, endpoints) -> List.copyOf(endpoints));
        view = new View(next, actions, freeze(services), freeze(topics));
        revision = assembling;
        chunks = null;
        version++;
        return true;
    }

    long version() { return version; }

    Endpoint instance(int id) { return view.instances.get(id); }
    Collection<Endpoint> endpoints() { return view.instances.values(); }
    List<Endpoint> service(String name) { return view.services.getOrDefault(name, List.of()); }
    List<Endpoint> topic(String name) { return view.topics.getOrDefault(name, List.of()); }

    Endpoint select(int action, int target, long key) {
        View snapshot = view;
        if (target != 0) {
            Endpoint endpoint = snapshot.instances.get(target);
            return endpoint != null && endpoint.actions().contains(action) ? endpoint : null;
        }
        Endpoint best = null;
        long bestScore = 0;
        for (Endpoint endpoint : snapshot.actions.getOrDefault(action, List.of())) {
            long score = score(key, endpoint.instanceId());
            if (best == null || Long.compareUnsigned(score, bestScore) > 0
                    || score == bestScore && endpoint.instanceId() < best.instanceId()) {
                best = endpoint;
                bestScore = score;
            }
        }
        return best;
    }

    private static long score(long key, int id) {
        long value = key ^ Integer.toUnsignedLong(id);
        value = (value ^ (value >>> 33)) * 0xff51afd7ed558ccdL;
        value = (value ^ (value >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return value ^ (value >>> 33);
    }

    private static <K> Map<K, List<Endpoint>> freeze(Map<K, List<Endpoint>> values) {
        values.replaceAll((key, list) -> List.copyOf(list));
        return Map.copyOf(values);
    }
    private record View(Int2ObjectHashMap<Endpoint> instances, Int2ObjectHashMap<List<Endpoint>> actions,
                        Map<String, List<Endpoint>> services, Map<String, List<Endpoint>> topics) {}
}
