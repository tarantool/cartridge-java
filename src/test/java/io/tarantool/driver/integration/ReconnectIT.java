package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
public class ReconnectIT extends SharedCartridgeContainer {

    private static final Logger logger = LoggerFactory.getLogger(ReconnectIT.class);

    private static String USER_NAME;
    private static String PASSWORD;

    @BeforeAll
    public static void setUp() throws TimeoutException {
        startCluster();

        WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer);
        waitingConsumer.waitUntil(f -> f.getUtf8String().contains("The cluster is balanced ok"));

        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    private boolean crudProcedureIsNotDefined;

    @Test
    public void test_should_reconnect_ifReconnectIsInvoked() throws InterruptedException {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = getTarantoolClient();
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            if (throwable.getMessage().contains("Procedure 'crud.select' is not defined")) {
                logger.error(throwable.getMessage());
                crudProcedureIsNotDefined = true;
            }
        });
        new Thread(() -> {
            for (int i = 0; i < 10000; i++) {
                client.space("test_space").select(Conditions.any()).join();
            }
        }).start();

        client.eval("rawset(_G, 'crud', nil)").join();
        Thread.sleep(500);
        client.eval("rawset(_G, 'crud', require('crud'))").join();
        Thread.sleep(500);

        assertFalse(crudProcedureIsNotDefined);
    }

    @Test
    public void test_should_reconnect_ifCrudProcedureIsNotDefined() throws Exception {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = getTarantoolClient();

        // getting all routers uuids
        final Set<String> routerUuids = getInstancesUuids(client);

        // stop routers
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "router");
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "second-router");

        // check that there is only one instance left
        assertEquals(getInstanceUuid(client), getInstanceUuid(client));

        // start routers
        container.execInContainer("cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d");

        client.establishLackingConnections();
        Thread.sleep(3000);

        // getting all routers uuids after restarting
        final Set<String> uuidsAfterReconnect = getInstancesUuids(client);

        // check that amount of routers is equal initial amount
        assertEquals(routerUuids.size(), uuidsAfterReconnect.size());
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getTarantoolClient() {
        return TarantoolClientFactory.createClient()
                .withAddresses(
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3311)),
                        new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3312))
                )
                .withCredentials(USER_NAME, PASSWORD)
                .withConnections(10)
                .withProxyMethodMapping()
                .withRetryingByNumberOfAttempts(5,
                        throwable -> throwable instanceof TarantoolNoSuchProcedureException,
                        factory -> factory.withDelay(100)
                )
                .build();
    }

    /**
     * Return all instances uuids from cluster, using round robin connection selection strategy
     *
     * @param client Tarantool client
     * @return set of instances uuids from cluster
     */
    private Set<String> getInstancesUuids(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        String firstUuid = getInstanceUuid(client);

        final Set<String> routerUuids = new HashSet<>();
        routerUuids.add(firstUuid);

        String currentUuid = "";
        while (!firstUuid.equals(currentUuid)) {
            currentUuid = getInstanceUuid(client);
            routerUuids.add(currentUuid);
        }

        return routerUuids;
    }

    private String getInstanceUuid(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return (String) client.eval("return box.info().uuid").join().get(0);
    }
}
