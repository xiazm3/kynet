package cn.itsub.kynet.registry;

public record RegistryClusterMetrics(long sendFailures, long authenticatedReplayRejected,
                                   int pendingSnapshots, int aliveMembers,
                                   int suspectMembers, int unavailableMembers) {}
