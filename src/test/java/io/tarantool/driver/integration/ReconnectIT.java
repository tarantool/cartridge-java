package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class ReconnectIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    @BeforeAll
    public static void setUp() throws TimeoutException {
        startCluster();

        WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer);
        waitingConsumer.waitUntil(f -> f.getUtf8String().contains("The cluster is balanced ok"));

        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    @Test
    public void test_should_reconnect_ifReconnectIsInvoked() throws Exception {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createClient()
                        .withAddresses(
                                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3311)),
                                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3312))
                        )
                        .withCredentials(USER_NAME, PASSWORD)
                        .build();

        final Set<String> routerUuids = getInstancesUuids(client);

        // stop routers
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "router");
        container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "second-router");

        assertEquals(client.eval("return box.info().uuid").join().get(0),
                client.eval("return box.info().uuid").join().get(0));

        // start routers
        container.execInContainer("cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d");

        client.establishLackingConnections();
        Thread.sleep(3000);

        final Set<String> uuidsAfterReconnect = getInstancesUuids(client);

        assertEquals(routerUuids.size(), uuidsAfterReconnect.size());
    }

    private Set<String> getInstancesUuids(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        final Set<String> routerUuids = new HashSet<>();
        String firstUuid = getInstanceUuid(client);
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
