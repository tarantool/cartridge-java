package io.tarantool.driver.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

abstract class SharedTarantoolContainer {

    private static final Logger logger = LoggerFactory.getLogger(SharedTarantoolContainer.class);

    protected static final TarantoolContainer container = new TarantoolContainer()
        .withScriptFileName("org/testcontainers/containers/server.lua")
        .withLogConsumer(new Slf4jLogConsumer(logger));

    protected static void startContainer() {
        if (!container.isRunning()) {
            container.start();
        }
    }
}
