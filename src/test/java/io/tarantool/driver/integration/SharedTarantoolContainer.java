package io.tarantool.driver.integration;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.junit.jupiter.Container;

abstract class SharedTarantoolContainer {

    protected static final String TEST_SPACE_NAME = "test_space";

    protected static final int DEFAULT_TEST_TIMEOUT = 5 * 1000;

    @Container
    protected static TarantoolContainer tarantoolContainer = new TarantoolContainer()
            .withScriptFileName("org/testcontainers/containers/server.lua");

    protected static TarantoolClient createClient() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), tarantoolContainer.getPort());

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(DEFAULT_TEST_TIMEOUT)
                .withReadTimeout(DEFAULT_TEST_TIMEOUT)
                .withRequestTimeout(DEFAULT_TEST_TIMEOUT)
                .build();

        return new StandaloneTarantoolClient(config, serverAddress);
    }
}
