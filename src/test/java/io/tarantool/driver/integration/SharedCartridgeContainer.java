package io.tarantool.driver.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.time.Duration;

abstract class SharedCartridgeContainer {

    private static final Logger logger = LoggerFactory.getLogger(SharedCartridgeContainer.class);

    protected static final TarantoolCartridgeContainer container =
            new TarantoolCartridgeContainer(
                    "cartridge/instances.yml",
                    "cartridge/topology.lua")
                    .withDirectoryBinding("cartridge")
                    .withLogConsumer(new Slf4jLogConsumer(logger))
                    .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 7))
                    .withStartupTimeout(Duration.ofMinutes(2));

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }
}
