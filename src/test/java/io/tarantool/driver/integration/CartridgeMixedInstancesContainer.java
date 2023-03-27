package io.tarantool.driver.integration;

import java.time.Duration;
import java.util.HashMap;
import io.tarantool.driver.TarantoolUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

abstract class CartridgeMixedInstancesContainer {

    private static final Logger logger = LoggerFactory.getLogger(CartridgeMixedInstancesContainer.class);
    private static final String tarantoolVersionSuffix = TarantoolUtils.getTarantoolVersionSuffix();
    private static final String instancesFile =
        String.format("cartridge/instances_mixed%s.yml", tarantoolVersionSuffix);
    private static final String topologyFile =
        String.format("cartridge/topology_mixed%s.lua", tarantoolVersionSuffix);

    protected static final int routerPort = tarantoolVersionSuffix == "_2" ? 3351 : 3341;
    protected static final int routerAPIPort = tarantoolVersionSuffix == "_2" ? 8581 : 8481;
    protected static final int router2Port = routerPort + 1;
    protected static final int router2APIPort = routerAPIPort + 1;
    protected static final int router3Port = routerPort + 2;
    protected static final int router3APIPort = routerAPIPort + 2;

    protected static final TarantoolCartridgeContainer container;

    static {
        final HashMap<String, String> env = new HashMap<>();
        env.put("TARANTOOL_INSTANCES_FILE", instancesFile.substring(instancesFile.lastIndexOf("/") + 1));
        container = new TarantoolCartridgeContainer(
            "Dockerfile",
            "cartridge-java-test-mixed",
            instancesFile,
            topologyFile,
            env)
            .withDirectoryBinding("cartridge")
            .withRouterPort(routerPort)
            .withAPIPort(routerAPIPort)
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
