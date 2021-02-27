package io.tarantool.driver.integration;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * @author Alexey Kuzin
 */
@Testcontainers
public class ClusterConnectionIT extends SharedCartridgeContainer {

    @BeforeAll
    public static void setUp() {
        // startCluster();
    }

    void testMultipleRoutersReconnect_retryableRequestShouldNotFail() {
        // create retrying client with two routers
        // initiate two long-running requests
        // shutdown one router
        // calculate the retry attempts -- for one of them they must be greater than 0, for another -- 0
        // both requests should return normally (alive connection selection effect)
        // startup the second router
        // initiate two requests
        // check that they completed normally without reties and fell onto different routers (reconnection effect)
    }

    void testMultipleConnectionsReconnect_retryableRequestShouldNotFail() {
        // create retrying client with one router and two connections
        // initiate two long-running requests
        // close one connection
        // calculate the retry attempts -- for one of them they must be greater than 0, for another -- 0
        // both requests should return normally (alive connection selection effect)
        // close the second connection
        // initiate two requests
        // check that they completed normally without retries and fell onto different connections (reconnection effect)
    }
}
