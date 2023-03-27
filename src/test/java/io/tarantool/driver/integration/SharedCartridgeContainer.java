package io.tarantool.driver.integration;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import io.tarantool.driver.TarantoolUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;


public abstract class SharedCartridgeContainer {

    private static final Logger logger = LoggerFactory.getLogger(SharedCartridgeContainer.class);
    private static final String tarantoolVersionSuffix = TarantoolUtils.getTarantoolVersionSuffix();
    private static final String instancesFileName = String.format("instances%s.yml", tarantoolVersionSuffix);
    private static final String instancesFile = String.format("cartridge/%s", instancesFileName);
    private static final String topologyFile = String.format("cartridge/topology%s.lua", tarantoolVersionSuffix);
    private static final int DEFAULT_START_ATTEMPTS = 30;
    private static final int DEFAULT_START_SLEEP = 1000; // ms

    protected static final int routerPort = tarantoolVersionSuffix == "_2" ? 3311 : 3301;
    protected static final int routerAPIPort = tarantoolVersionSuffix == "_2" ? 8181 : 8081;
    protected static final int router2Port = routerPort + 1;
    protected static final int router2APIPort = routerAPIPort + 1;
    protected static final int router3Port = routerPort + 2;
    protected static final int router3APIPort = routerAPIPort + 2;

    protected static final TarantoolCartridgeContainer container;

    static {
        final HashMap<String, String> env = new HashMap<>();
        env.put("TARANTOOL_INSTANCES_FILE", instancesFileName);
        container = new TarantoolCartridgeContainer(
            "Dockerfile",
            "cartridge-java-test",
            instancesFile,
            topologyFile,
            env)
            .withDirectoryBinding("cartridge")
            .withAPIPort(routerAPIPort)
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 5))
            .withStartupTimeout(Duration.ofMinutes(2));
    }

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }

    protected static void startCartridge() throws IOException, InterruptedException {
        container.execInContainer("cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data",
            "--log-dir=/tmp/log", "--cfg=" + instancesFileName, "-d");
    }

    protected static void stopCartridge() throws IOException, InterruptedException {
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "--cfg=" + instancesFileName);
    }

    protected static void startInstances(List<String> instancesName) throws IOException, InterruptedException {
        for (String instanceName : instancesName) {
            startInstance(instanceName);
        }
    }

    protected static void stopInstances(List<String> instancesName) throws IOException, InterruptedException {
        for (String instanceName : instancesName) {
            stopInstance(instanceName, false);
        }
    }

    protected static ExecResult startInstance(String instanceName) throws IOException, InterruptedException {
        ExecResult result = null;
        int attempts = DEFAULT_START_ATTEMPTS;
        while (attempts > 0) {
            container.execInContainer(
                "cartridge", "clean", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "--log-dir=/tmp/log",
                "--cfg=" + instancesFileName, instanceName);
            result = container.execInContainer(
                "cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "--log-dir=/tmp/log",
                "--cfg=" + instancesFileName, "-d", instanceName);
            if (!result.getStderr().contains("Process is already running")) {
                break;
            }
            logger.info(
                "Instance {} has not yet existed completely, sleeping {} ms", instanceName, DEFAULT_START_SLEEP);
            Thread.sleep(DEFAULT_START_SLEEP);
            attempts--;
        }
        return result;
    }

    protected static ExecResult stopInstance(String instanceName, boolean force)
        throws IOException, InterruptedException {
        if (force) {
            return container.execInContainer(
                "cartridge", "stop", "--run-dir=/tmp/run", "--cfg=" + instancesFileName, "--force", instanceName);
        } else {
            return container.execInContainer(
                "cartridge", "stop", "--run-dir=/tmp/run", "--cfg=" + instancesFileName, instanceName);
        }
    }
}
