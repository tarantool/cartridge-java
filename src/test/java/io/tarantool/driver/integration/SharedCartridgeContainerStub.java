package io.tarantool.driver.integration;

import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a debugging stub that mimics a tarantool
 * testcontainer but works with a running cluster.
 *
 * @author Vladimir Rogach
 */
abstract class SharedCartridgeContainerStub {

    private static final Logger logger = LoggerFactory.getLogger(SharedCartridgeContainerStub.class);

    private static String TARANTOOL_HOST = "localhost";
    private static int TARANTOOL_PORT = 3301;
    private static String TARANTOOL_USERNAME = "admin";
    private static String TARANTOOL_PASSWORD = "testapp-cluster-cookie";

    @ClassRule
    protected static final TarantoolContainerStub container =
            new TarantoolContainerStub();

    protected static void startCluster() {
        logger.info("STUB starting cluster.");
    }

    public static class TarantoolContainerStub {

        public String getUsername() {
            return TARANTOOL_USERNAME;
        }

        public String getPassword() {
            return TARANTOOL_PASSWORD;
        }

        public String getRouterHost() {
            return TARANTOOL_HOST;
        }

        public int getRouterPort() {
            return TARANTOOL_PORT;
        }
    }
}
