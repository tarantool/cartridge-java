package io.tarantool.driver.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

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
