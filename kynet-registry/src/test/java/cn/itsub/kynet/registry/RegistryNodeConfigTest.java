package cn.itsub.kynet.registry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RegistryNodeConfigTest {
    private static final String[] PROPERTIES = {
        "kynet.broker.cluster.enabled", "kynet.broker.cluster.secret", "kynet.broker.clientHost",
        "kynet.broker.clusterHost", "kynet.broker.clusterPort", "kynet.broker.discoveryPort",
        "kynet.broker.suspectTimeoutMs", "kynet.broker.deadTimeoutMs", "kynet.broker.cluster.gossipRoutesPerChunk",
        "kynet.registry.clientHost"
    };
    @AfterEach void clearProperties() { for (String key : PROPERTIES) System.clearProperty(key); }
    @Test void rejectsInvalidFailureDetectorWindow() {
        defaults();
        System.setProperty("kynet.broker.suspectTimeoutMs", "200");
        System.setProperty("kynet.broker.deadTimeoutMs", "200");
        assertThrows(IllegalStateException.class, () -> RegistryNodeConfig.fromSystemProperties(19101));
    }
    @Test void rejectsOversizedDirectoryChunks() {
        defaults();
        System.setProperty("kynet.broker.cluster.gossipRoutesPerChunk", "1025");
        assertThrows(IllegalStateException.class, () -> RegistryNodeConfig.fromSystemProperties(19101));
    }
    @Test void rejectsWildcardAdvertisedAddress() {
        defaults();
        System.setProperty("kynet.broker.clientHost", "0.0.0.0");
        assertThrows(IllegalStateException.class, () -> RegistryNodeConfig.fromSystemProperties(19101));
    }
    @Test void rejectsInvalidPort() {
        defaults();
        assertThrows(IllegalStateException.class, () -> RegistryNodeConfig.fromSystemProperties(65536));
    }
    @Test void registryPropertyOverridesLegacyBrokerProperty() {
        defaults();
        System.setProperty("kynet.broker.clientHost", "0.0.0.0");
        System.setProperty("kynet.registry.clientHost", "127.0.0.1");
        assertEquals("127.0.0.1", RegistryNodeConfig.fromSystemProperties(19101).clientHost());
    }
    private static void defaults() {
        System.setProperty("kynet.broker.cluster.enabled", "true");
        System.setProperty("kynet.broker.cluster.secret", "node-config-test-secret");
        System.setProperty("kynet.broker.clientHost", "127.0.0.1");
        System.setProperty("kynet.broker.clusterHost", "127.0.0.1");
        System.setProperty("kynet.broker.clusterPort", "19201");
        System.setProperty("kynet.broker.discoveryPort", "19301");
    }
}
