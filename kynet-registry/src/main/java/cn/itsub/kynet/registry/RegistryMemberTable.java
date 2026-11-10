package cn.itsub.kynet.registry;

import cn.itsub.kynet.common.protocol.RegistryTopologyCodec;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

final class RegistryMemberTable {
    private final RegistryNodeConfig config;
    private final Map<String, RegistryMember> members = new ConcurrentHashMap<>();
    private final AtomicLong localVersion = new AtomicLong(1);
    private final AtomicLong localIncarnation = new AtomicLong(1);

    RegistryMemberTable(RegistryNodeConfig config) {
        this.config = config;
        RegistryMember self = new RegistryMember(
                config.nodeId(),
                config.epoch(),
                config.clusterHost(),
                config.clientHost(),
                config.clientPort(),
                config.clusterPort(),
                localIncarnation.get(),
                MemberState.ALIVE,
                localVersion.get(),
                System.currentTimeMillis());
        members.put(config.nodeId(), self);
    }

    RegistryMember self() {
        return members.get(config.nodeId());
    }

    boolean mergeDiscovery(ClusterMessageCodec.DiscoveryMessage message) {
        if (message == null || config.nodeId().equals(message.fromNodeId())) {
            return false;
        }
        return mergeDirect(new RegistryMember(
                message.fromNodeId(),
                message.fromEpoch(),
                message.host(),
                message.clientHost(),
                message.clientPort(),
                message.clusterPort(),
                message.incarnation(),
                MemberState.ALIVE,
                message.version(),
                System.currentTimeMillis()));
    }

    boolean markDirectlySeen(String nodeId, long epoch) {
        if (nodeId == null || config.nodeId().equals(nodeId)) {
            return false;
        }
        AtomicBooleanBox changed = new AtomicBooleanBox();
        long now = System.currentTimeMillis();
        members.computeIfPresent(nodeId, (ignored, current) -> {
            if (current.epoch() != epoch) {
                return current;
            }
            if (current.state() != MemberState.ALIVE) {
                changed.value = true;
            }
            return new RegistryMember(current.nodeId(), current.epoch(), current.host(), current.clientHost(),
                    current.clientPort(), current.clusterPort(), current.incarnation(),
                    MemberState.ALIVE, current.version(), now);
        });
        return changed.value;
    }

    private boolean mergeDirect(RegistryMember incoming) {
        AtomicBooleanBox changed = new AtomicBooleanBox();
        long now = System.currentTimeMillis();
        members.compute(incoming.nodeId(), (nodeId, current) -> {
            if (current == null || incoming.epoch() > current.epoch()
                    || (incoming.epoch() == current.epoch()
                    && incoming.incarnation() > current.incarnation())) {
                changed.value = true;
                return incoming.withLastSeen(now);
            }
            if (incoming.epoch() < current.epoch() || incoming.incarnation() < current.incarnation()) {
                return current;
            }
            if (current.state() != MemberState.ALIVE
                    || !current.host().equals(incoming.host())
                    || !current.clientHost().equals(incoming.clientHost())
                    || current.clientPort() != incoming.clientPort()
                    || current.clusterPort() != incoming.clusterPort()) {
                changed.value = true;
            }
            return new RegistryMember(
                    current.nodeId(), current.epoch(), incoming.host(), incoming.clientHost(),
                    incoming.clientPort(), incoming.clusterPort(), current.incarnation(), MemberState.ALIVE,
                    Math.max(current.version(), incoming.version()), now);
        });
        return changed.value;
    }

    boolean mergeRumors(List<RegistryMember> snapshots) {
        boolean changed = false;
        for (RegistryMember snapshot : snapshots) {
            changed |= mergeRumor(snapshot);
        }
        return changed;
    }

    private boolean mergeRumor(RegistryMember incoming) {
        if (incoming == null || incoming.nodeId() == null || incoming.nodeId().isBlank()) {
            return false;
        }
        if (config.nodeId().equals(incoming.nodeId())) {
            if (incoming.epoch() == config.epoch()
                    && (incoming.state() == MemberState.SUSPECT || incoming.state() == MemberState.DEAD)) {
                markSelfAliveAbove(incoming.incarnation(), incoming.version());
                return true;
            }
            return false;
        }
        AtomicBooleanBox changed = new AtomicBooleanBox();
        members.compute(incoming.nodeId(), (nodeId, current) -> {
            if (current == null) {
                changed.value = true;
                return incoming.withLastSeen(0L);
            }
            if (incoming.isNewerThan(current)) {
                changed.value = true;
                RegistryMember updated = incoming.withLastSeen(current.lastSeenMillis());
                return incoming.epoch() == current.epoch()
                        ? updated.withNetworkMetadataFrom(current)
                        : updated;
            }
            return current;
        });
        return changed.value;
    }

    private void markSelfAliveAbove(long observedIncarnation, long observedVersion) {
        long incarnation = localIncarnation.updateAndGet(current -> Math.max(current, observedIncarnation) + 1);
        long version = localVersion.updateAndGet(current -> Math.max(current, observedVersion) + 1);
        members.put(config.nodeId(), new RegistryMember(
                config.nodeId(),
                config.epoch(),
                config.clusterHost(),
                config.clientHost(),
                config.clientPort(),
                config.clusterPort(),
                incarnation,
                MemberState.ALIVE,
                version,
                System.currentTimeMillis()));
    }

    boolean markLeft() {
        long incarnation = localIncarnation.incrementAndGet();
        long version = localVersion.incrementAndGet();
        members.put(config.nodeId(), new RegistryMember(
                config.nodeId(),
                config.epoch(),
                config.clusterHost(),
                config.clientHost(),
                config.clientPort(),
                config.clusterPort(),
                incarnation,
                MemberState.LEFT,
                version,
                System.currentTimeMillis()));
        return true;
    }

    boolean detectFailures(long nowMillis) {
        boolean changed = false;
        for (RegistryMember member : members.values()) {
            if (config.nodeId().equals(member.nodeId())
                    || member.state() == MemberState.DEAD
                    || member.state() == MemberState.LEFT) {
                continue;
            }
            long idleMillis = nowMillis - member.lastSeenMillis();
            if (idleMillis >= config.deadTimeoutMillis()) {
                changed |= updateState(member, MemberState.DEAD);
            } else if (member.state() == MemberState.ALIVE
                    && idleMillis >= config.suspectTimeoutMillis()) {
                changed |= updateState(member, MemberState.SUSPECT);
            }
        }
        return changed;
    }

    private boolean updateState(RegistryMember member, MemberState state) {
        AtomicBooleanBox changed = new AtomicBooleanBox();
        members.computeIfPresent(member.nodeId(), (nodeId, current) -> {
            if (current.state() == state || current.epoch() != member.epoch()) {
                return current;
            }
            changed.value = true;
            return current.withState(state, current.version() + 1);
        });
        return changed.value;
    }

    List<RegistryMember> snapshots() {
        return members.values().stream()
                .sorted(Comparator.comparing(RegistryMember::nodeId))
                .toList();
    }

    List<RegistryMember> gossipTargets() {
        return members.values().stream()
                .filter(member -> !config.nodeId().equals(member.nodeId()))
                .filter(member -> member.state() == MemberState.ALIVE || member.state() == MemberState.SUSPECT)
                .sorted(Comparator.comparing(RegistryMember::nodeId))
                .toList();
    }

    RegistryMember member(String nodeId) {
        return members.get(nodeId);
    }

    boolean isAlive(String nodeId, long epoch) {
        RegistryMember member = members.get(nodeId);
        return member != null
                && member.epoch() == epoch
                && member.state() == MemberState.ALIVE;
    }

    List<RegistryTopologyCodec.Endpoint> topologyEndpoints() {
        List<RegistryTopologyCodec.Endpoint> endpoints = new ArrayList<>();
        for (RegistryMember member : snapshots()) {
            endpoints.add(new RegistryTopologyCodec.Endpoint(
                    member.nodeId(),
                    member.clientHost(),
                    member.clientPort(),
                    member.state().name()));
        }
        return endpoints;
    }

    private static final class AtomicBooleanBox {
        private boolean value;
    }
}

record RegistryMember(String nodeId, long epoch, String host, String clientHost,
                    int clientPort, int clusterPort,
                    long incarnation, MemberState state, long version, long lastSeenMillis) {
    RegistryMember(String nodeId, long epoch, String host, int clientPort, int clusterPort,
                 long incarnation, MemberState state, long version, long lastSeenMillis) {
        this(nodeId, epoch, host, host, clientPort, clusterPort,
                incarnation, state, version, lastSeenMillis);
    }

    boolean isNewerThan(RegistryMember current) {
        if (epoch != current.epoch()) {
            return epoch > current.epoch();
        }
        if (incarnation != current.incarnation()) {
            return incarnation > current.incarnation();
        }
        if (version != current.version()) {
            return version > current.version();
        }
        return statePrecedence(state) > statePrecedence(current.state());
    }

    RegistryMember withLastSeen(long nowMillis) {
        return new RegistryMember(nodeId, epoch, host, clientHost, clientPort, clusterPort,
                incarnation, state, version, nowMillis);
    }

    RegistryMember withState(MemberState newState, long newVersion) {
        return new RegistryMember(nodeId, epoch, host, clientHost, clientPort, clusterPort,
                incarnation, newState, newVersion, lastSeenMillis);
    }

    RegistryMember withNetworkMetadataFrom(RegistryMember source) {
        return new RegistryMember(nodeId, epoch, source.host, source.clientHost,
                source.clientPort, source.clusterPort,
                incarnation, state, version, lastSeenMillis);
    }

    private static int statePrecedence(MemberState state) {
        return switch (state) {
            case ALIVE -> 1;
            case SUSPECT -> 2;
            case DEAD -> 3;
            case LEFT -> 4;
        };
    }
}
