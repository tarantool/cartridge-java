package io.tarantool.driver.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.HashMap;

abstract class CartridgeMixedInstancesContainer {

    private static final Logger logger = LoggerFactory.getLogger(CartridgeMixedInstancesContainer.class);

    protected static final TarantoolCartridgeContainer container;

    static {
        final HashMap<String, String> buildArgs = new HashMap<>();
        buildArgs.put("TARANTOOL_INSTANCES_FILE", "./instances_mixed.yml");
        container = new TarantoolCartridgeContainer(
                "cartridge/instances_mixed.yml",
                "cartridge/topology_mixed.lua", buildArgs)
                .withDirectoryBinding("cartridge")
                .withLogConsumer(new Slf4jLogConsumer(logger))
                .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 3))
                .withStartupTimeout(Duration.ofMinutes(2));
    }

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }
}
