package io.tarantool.driver.integration;

import java.time.Duration;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

abstract class CartridgeMixedInstancesContainer {

    private static final Logger logger = LoggerFactory.getLogger(CartridgeMixedInstancesContainer.class);

    protected static final TarantoolCartridgeContainer container;

    static {
        final HashMap<String, String> env = new HashMap<>();
        env.put("TARANTOOL_INSTANCES_FILE", "./instances_mixed.yml");
        container = new TarantoolCartridgeContainer("cartridge/instances_mixed.yml",
                                                    "cartridge/topology_mixed.lua")
                        .withDirectoryBinding("cartridge")
                        .withLogConsumer(new Slf4jLogConsumer(logger))
                        .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 3))
                        .withStartupTimeout(Duration.ofMinutes(2))
                        .withEnv(env);
    }

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }
}
