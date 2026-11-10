package cn.itsub.kynet.registry;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

final class RegistryClusterRouteTable {
    private final String localNodeId;
    private final long localEpoch;
    private final AtomicLong localSnapshotVersion = new AtomicLong();
    private final Map<String, ClusterRoute> routes = new ConcurrentHashMap<>();
    private final Map<Integer, Set<String>> localRouteKeysByInstanceId = new ConcurrentHashMap<>();
    private final Map<String, OwnerSnapshot> remoteSnapshots = new HashMap<>();

    RegistryClusterRouteTable(String localNodeId, long localEpoch) {
        this.localNodeId = localNodeId;
        this.localEpoch = localEpoch;
    }

    ClusterRoute upAction(InstanceMeta meta, int action) {
        ClusterRoute route = ClusterRoute.action(
                localNodeId,
                localEpoch,
                meta.instanceId(),
                meta.instanceGeneration(),
                meta.serviceName(),
                meta.serviceId(),
                action,
                nextVersion());
        putLocal(route);
        return route;
    }

    void upEndpoint(InstanceMeta meta, String channel, int streamId, long incarnation) {
        putLocal(new ClusterRoute("S:" + meta.instanceId(), RouteKind.SERVICE, localNodeId, localEpoch,
                meta.instanceId(), meta.instanceGeneration(), meta.serviceName(), meta.serviceId(),
                0, nextVersion(), channel, streamId, incarnation, ""));
    }

    void upTopic(InstanceMeta meta, String topic) {
        putLocal(new ClusterRoute("T:" + topic + ":" + meta.instanceId(), RouteKind.TOPIC, localNodeId, localEpoch,
                meta.instanceId(), meta.instanceGeneration(), meta.serviceName(), meta.serviceId(),
                0, nextVersion(), "", 0, 0, topic));
    }

    void downInstance(int instanceId) {
        Set<String> keys = localRouteKeysByInstanceId.remove(instanceId);
        if (keys == null || keys.isEmpty()) {
            return;
        }
        boolean removed = false;
        for (String key : keys) {
            removed |= routes.remove(scopedKey(localNodeId, key)) != null;
        }
        if (removed) {
            nextVersion();
        }
    }

    private void putLocal(ClusterRoute route) {
        routes.put(scopedKey(route.ownerNodeId(), route.key()), route);
        localRouteKeysByInstanceId
                .computeIfAbsent(route.instanceId(), ignored -> ConcurrentHashMap.newKeySet())
                .add(route.key());
    }

    synchronized boolean replaceRemoteSnapshot(String ownerNodeId, long ownerEpoch,
                                               long snapshotVersion, List<ClusterRoute> incomingRoutes) {
        if (ownerNodeId == null || ownerNodeId.isBlank() || localNodeId.equals(ownerNodeId)
                || snapshotVersion < 0 || incomingRoutes == null) {
            return false;
        }
        OwnerSnapshot current = remoteSnapshots.get(ownerNodeId);
        if (current != null) {
            if (ownerEpoch < current.epoch()
                    || (ownerEpoch == current.epoch() && snapshotVersion <= current.version())) {
                return false;
            }
        }
        for (ClusterRoute route : incomingRoutes) {
            if (route == null || !ownerNodeId.equals(route.ownerNodeId()) || route.ownerEpoch() != ownerEpoch) {
                return false;
            }
        }

        routes.entrySet().removeIf(entry -> ownerNodeId.equals(entry.getValue().ownerNodeId()));
        for (ClusterRoute route : incomingRoutes) {
            routes.put(scopedKey(ownerNodeId, route.key()), route);
        }
        remoteSnapshots.put(ownerNodeId, new OwnerSnapshot(ownerEpoch, snapshotVersion));
        return true;
    }

    synchronized boolean hasRemoteSnapshot(String ownerNodeId, long ownerEpoch) {
        OwnerSnapshot snapshot = remoteSnapshots.get(ownerNodeId);
        return snapshot != null && snapshot.epoch() == ownerEpoch;
    }

    synchronized void removeRoutesForNode(String nodeId, long epoch) {
        routes.entrySet().removeIf(entry -> {
            ClusterRoute route = entry.getValue();
            return route.ownerNodeId().equals(nodeId) && route.ownerEpoch() == epoch;
        });
        OwnerSnapshot snapshot = remoteSnapshots.get(nodeId);
        if (snapshot != null && snapshot.epoch() == epoch) {
            remoteSnapshots.remove(nodeId);
        }
    }

    long localSnapshotVersion() {
        return localSnapshotVersion.get();
    }

    List<ClusterRoute> localSnapshots() {
        return routes.values().stream()
                .filter(route -> localNodeId.equals(route.ownerNodeId()))
                .sorted(Comparator.comparing(ClusterRoute::key))
                .toList();
    }

    List<ClusterRoute> snapshots() {
        return routes.values().stream()
                .sorted(Comparator.comparing(ClusterRoute::ownerNodeId).thenComparing(ClusterRoute::key))
                .toList();
    }

    private long nextVersion() {
        return localSnapshotVersion.incrementAndGet();
    }

    private static String scopedKey(String ownerNodeId, String routeKey) {
        return ownerNodeId + '\u0000' + routeKey;
    }

    private record OwnerSnapshot(long epoch, long version) {
    }
}

record ClusterRoute(String key, RouteKind kind, String ownerNodeId, long ownerEpoch, int instanceId,
                    long instanceGeneration, String serviceName, int serviceId, int action,
                    long version, String endpointChannel, int endpointStreamId, long incarnation, String topicName) {
    static ClusterRoute action(String ownerNodeId, long ownerEpoch, int instanceId, long generation,
                               String serviceName, int serviceId, int action, long version) {
        return new ClusterRoute("A:" + action + ":" + instanceId, RouteKind.ACTION, ownerNodeId, ownerEpoch,
                instanceId, generation, serviceName, serviceId, action, version, "", 0, 0L, "");
    }
}
