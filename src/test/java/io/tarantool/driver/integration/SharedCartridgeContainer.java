package io.tarantool.driver.integration;

import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

abstract class SharedCartridgeContainer {

    private static final Logger logger = LoggerFactory.getLogger(SharedCartridgeContainer.class);

    @ClassRule
    protected static final TarantoolCartridgeContainer container =
            (TarantoolCartridgeContainer) new TarantoolCartridgeContainer(
            "cartridge/instances.yml",
            "cartridge/topology.lua")
            .withDirectoryBinding("cartridge")
            .cleanUpDirectory("cartridge/tmp")
            .withLogConsumer(new Slf4jLogConsumer(logger));

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }
}
