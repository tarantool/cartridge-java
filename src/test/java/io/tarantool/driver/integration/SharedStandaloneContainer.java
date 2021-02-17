package io.tarantool.driver.integration;


import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.junit.jupiter.Container;

import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class SharedStandaloneContainer {

    private static final Logger log = LoggerFactory.getLogger(SharedStandaloneContainer.class);

    @Container
    protected static final TarantoolContainer tarantoolContainer = new TarantoolContainer()
            .withScriptFileName("org/testcontainers/containers/server.lua");

    protected static TarantoolClient client;

    public static void setUp() {
        assertTrue(tarantoolContainer.isRunning());
        initClient();
    }

    public static void tearDown() throws Exception {
        client.close();
    }

    private static void initClient() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), tarantoolContainer.getPort());

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .build();

        log.info("Attempting connect to Tarantool");
        client = new StandaloneTarantoolClient(config, serverAddress);
        log.info("Successfully connected to Tarantool, version = {}", client.getVersion());
    }
}
