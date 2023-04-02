package io.tarantool.driver.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;

abstract class SharedTarantoolContainer {

    private static final Logger logger = LoggerFactory.getLogger(SharedTarantoolContainer.class);
    protected static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    protected static final String tarantoolVersion = System.getenv().get("TARANTOOL_VERSION");
    protected static final TarantoolContainer container =
        new TarantoolContainer(
            String.format("tarantool/tarantool:%s-centos7",
                tarantoolVersion != null ? tarantoolVersion : "2.10.5"))
        .withScriptFileName("org/testcontainers/containers/server.lua")
        .withLogConsumer(new Slf4jLogConsumer(logger));

    protected static void startContainer() {
        if (!container.isRunning()) {
            container.start();
        }
    }

    protected static void initClient() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            container.getUsername(), container.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                                           .withCredentials(credentials)
                                           .withConnectTimeout(1000 * 5)
                                           .withReadTimeout(1000 * 5)
                                           .withRequestTimeout(1000 * 5)
                                           .build();

        logger.info("Attempting connect to Tarantool");
        client = new ClusterTarantoolTupleClient(config, serverAddress);
        logger.info("Successfully connected to Tarantool, version = {}", client.getVersion());
    }
}
