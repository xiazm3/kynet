package cn.itsub.kynet.registry;

public record RegistryMetrics(int registeredInstances, long registrations,
                                    long controlMessagesSent, long controlMessagesBuffered,
                                    long controlMessagesBackpressured, long rejectedMessages) {}
