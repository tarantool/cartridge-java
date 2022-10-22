package io.tarantool.driver.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

abstract class SharedTarantoolContainer {

    private static final Logger logger = LoggerFactory.getLogger(SharedTarantoolContainer.class);

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
}
