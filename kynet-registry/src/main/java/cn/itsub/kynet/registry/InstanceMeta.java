package cn.itsub.kynet.registry;

public record InstanceMeta(String serviceName, int serviceId, int instanceId, long instanceGeneration) {
    public InstanceMeta(String serviceName, int serviceId, int instanceId) {
        this(serviceName, serviceId, instanceId, 1L);
    }
}
