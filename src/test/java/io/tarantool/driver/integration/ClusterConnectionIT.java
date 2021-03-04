package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.RetryingTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolRequestRetryPolicies;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Alexey Kuzin
 */
@Testcontainers
public class ClusterConnectionIT extends SharedCartridgeContainer {

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

    private TarantoolClientConfig.Builder prepareConfig() {
        return TarantoolClientConfig.builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000)
                .withReadTimeout(1000);
    }

    private ProxyTarantoolTupleClient setupRouterClient(int port) {
        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
                prepareConfig().build(), container.getRouterHost(), container.getMappedPort(port));

        return new ProxyTarantoolTupleClient(clusterClient);
    }

    private RetryingTarantoolTupleClient setupRetryingClient(TarantoolClientConfig config,
                                                             TarantoolClusterAddressProvider addressProvider,
                                                             int retries, long delay) {
        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(config, addressProvider);

        ProxyTarantoolTupleClient client = new ProxyTarantoolTupleClient(clusterClient);
        return new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies.byNumberOfAttempts(retries).withDelay(delay).build());
    }

    @Test
    void testMultipleRoutersConnectWithUnreachable_retryableRequestShouldNotFail() throws Exception {
        // create retrying client with two routers, one does not exist
        TarantoolClusterAddressProvider addressProvider = () -> Arrays.asList(
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                new TarantoolServerAddress(container.getRouterHost(), 33399));

        RetryingTarantoolTupleClient client = setupRetryingClient(prepareConfig().build(), addressProvider, 1, 0);

        assertDoesNotThrow(() -> client.call("reset_request_counters").get());
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());

        client.close();
    }

    void testMultipleRoutersReconnect_retryableRequestShouldNotFail()
            throws Exception {
        ProxyTarantoolTupleClient routerClient1 = setupRouterClient(3301);
        routerClient1.call("reset_request_counters").get();
        ProxyTarantoolTupleClient routerClient2 = setupRouterClient(3311);
        routerClient2.call("reset_request_counters").get();

        // create retrying client with two routers
        TarantoolClusterAddressProvider addressProvider = () -> Arrays.asList(
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3311)));

        RetryingTarantoolTupleClient client = setupRetryingClient(
                prepareConfig().withConnectTimeout(1000).build(), addressProvider, 10, 0);

        // initiate two long-running requests
        CompletableFuture<Boolean> request1 = client.callForSingleResult(
                "long_running_function", Collections.singletonList(0.5), Boolean.class);
        CompletableFuture<Boolean> request2 = client.callForSingleResult(
                "long_running_function", Collections.singletonList(0.5), Boolean.class);

        WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer);
        waitingConsumer.waitUntil(f -> f.getUtf8String().contains("Executing long-running function"));

        // shutdown one router
        Container.ExecResult result;
        result = container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "--force", "second-router");
        assertEquals(0, result.getExitCode(), result.getStderr());

        // both requests should return normally (alive connection selection effect)
        assertTrue(request1.get());
        assertTrue(request2.get());

        // shutdown the first router and startup both
        result = container.execInContainer("cartridge", "stop", "--run-dir=/tmp/run", "router");
        assertEquals(0, result.getExitCode(), result.getStderr());
        result = container.execInContainer("cartridge", "start", "--run-dir=/tmp/run", "router");
        assertEquals(0, result.getExitCode(), result.getStderr());
        result = container.execInContainer("cartridge", "start", "--run-dir=/tmp/run", "second-router");
        assertEquals(0, result.getExitCode(), result.getStderr());

        // get the request attempts -- for one of them they must be greater than 1, for another -- 1
        // the router clients must reconnect automatically
        assertEquals(2, routerClient1.callForSingleResult("get_request_count", Integer.class).get());
        assertEquals(1, routerClient2.callForSingleResult("get_request_count", Integer.class).get());

        // initiate two requests
//        routerClient1.call("reset_request_counters").get();
//        routerClient2.call("reset_request_counters").get();
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());

        // check that they completed normally without reties and fell onto different routers (full reconnection effect)
        assertEquals(3, routerClient1.callForSingleResult("get_request_count", Integer.class).get());
        assertEquals(2, routerClient2.callForSingleResult("get_request_count", Integer.class).get());

        routerClient1.close();
        routerClient2.close();
        client.close();
    }

    void testMultipleConnectionsReconnect_retryableRequestShouldNotFail() {
        // create retrying client with one router and two connections
        // initiate two long-running requests
        // close one connection
        // calculate the retry attempts -- for one of them they must be greater than 0, for another -- 0
        // both requests should return normally (alive connection selection effect)
        // close the second connection
        // initiate two requests
        // check that they completed normally without retries and fell onto different connections (reconnection effect)
    }
}
