package cn.itsub.kynet.framework.gateway;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionIdGeneratorTest {

    @AfterEach
    void clearWorkerId() {
        System.clearProperty("kynet.gateway.workerId");
    }

    @Test
    void configuredWorkerIdIsUsed() {
        System.setProperty("kynet.gateway.workerId", "512");

        assertEquals(512L, SessionIdGenerator.resolveWorkerId(123));
    }

    @Test
    void invalidConfiguredWorkerIdFailsFast() {
        System.setProperty("kynet.gateway.workerId", "1024");

        assertThrows(IllegalArgumentException.class, () -> SessionIdGenerator.resolveWorkerId(123));
    }

    @Test
    void fallbackWorkerIdIsHashedIntoTenBitRange() {
        long workerId = SessionIdGenerator.resolveWorkerId(-123456789);

        assertTrue(workerId >= 0);
        assertTrue(workerId <= 1023);
    }

    @Test
    void generatedIdsIncrease() {
        SessionIdGenerator generator = new SessionIdGenerator(1);

        long first = generator.nextId();
        long second = generator.nextId();

        assertTrue(second > first);
    }
}
