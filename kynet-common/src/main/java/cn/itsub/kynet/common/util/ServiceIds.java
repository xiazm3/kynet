package cn.itsub.kynet.common.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.CRC32;

public final class ServiceIds {
    private ServiceIds() {
    }

    public static int serviceId(String serviceName) {
        CRC32 crc32 = new CRC32();
        crc32.update(serviceName.getBytes(StandardCharsets.UTF_8));
        return (int) crc32.getValue();
    }

    public static int newInstanceId() {
        int instanceId;
        do {
            instanceId = UUID.randomUUID().hashCode();
        } while (instanceId == 0);
        return instanceId;
    }
}
