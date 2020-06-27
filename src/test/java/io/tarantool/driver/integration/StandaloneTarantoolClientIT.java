package io.tarantool.driver.integration;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClientException;
import io.tarantool.driver.TarantoolConnection;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class StandaloneTarantoolClientIT {

    private static Logger log = LoggerFactory.getLogger(StandaloneTarantoolClientIT.class);

    @Container
    private static final TarantoolContainer tarantoolContainer = new TarantoolContainer();

    @BeforeAll
    public static void setUp() {
        assertTrue(tarantoolContainer.isRunning());
    }

    @Test
    public void connectAndCheckMetadata() throws TarantoolClientException, Exception {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                //"guest", "");
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());
        log.info("Attempting connect to Tarantool");
        TarantoolConnection conn = new StandaloneTarantoolClient(credentials)
                .connect(tarantoolContainer.getHost(), tarantoolContainer.getPort());
        log.info("Successfully connected to Tarantool, version = {}", conn.getVersion());
        assertTrue(conn.metadata().getSpaceByName("_space").isPresent(), "Failed to get space metadata");
        log.info("Retrieved ID from metadata for space '_space': {}",
                conn.metadata().getSpaceByName("_space").get().getSpaceId());
        conn.close();
    }
}
