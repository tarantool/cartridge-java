package io.tarantool.driver.cluster;

import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Dmitry Kasimovskiy
 */
public class TarantoolClusterDiscoveryConfigTest {

    @Test
    public void test_withEndpoint_shouldReturnConfig() {
        // given
        TarantoolClusterDiscoveryEndpoint endpoint = new TarantoolClusterDiscoveryEndpoint() { };

        // when
        TarantoolClusterDiscoveryConfig cfg = new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .build();

        // then
        assertNotNull(cfg);
        assertEquals(endpoint, cfg.getEndpoint());
    }

    @Test
    public void test_withEndpoint_shouldThrowException_ifCalledMultipleTimes() {
        // given
        TarantoolClusterDiscoveryEndpoint endpoint = new TarantoolClusterDiscoveryEndpoint() { };

        // then
        assertThrows(TarantoolClientException.class, () -> new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withEndpoint(endpoint)
                .build());
    }
}
