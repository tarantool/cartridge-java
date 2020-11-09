package io.tarantool.driver.integration;

import org.junit.ClassRule;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.TarantoolContainer;

abstract class SharedCartridgeContainer {

    @ClassRule
    protected static final TarantoolCartridgeContainer container = new TarantoolCartridgeContainer(
            "cartridge/instances.yml",
            "cartridge/topology.lua")
            .withDirectoryBinding("cartridge")
            .cleanUpDirectory("cartridge/tmp");

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }
}
