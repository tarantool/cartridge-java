package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.TarantoolConnection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
@Testcontainers
public class ClusterConnectionIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    private static final Logger logger = LoggerFactory.getLogger(ClusterConnectionIT.class);

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

    private RetryingTarantoolTupleClient setupRouterClient(int port, int retries, long delay) {
        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
                prepareConfig().build(), container.getRouterHost(), container.getMappedPort(port));

        return new RetryingTarantoolTupleClient(new ProxyTarantoolTupleClient(clusterClient),
                TarantoolRequestRetryPolicies.byNumberOfAttempts(retries).withDelay(delay).build());
    }

    private RetryingTarantoolTupleClient setupClusterClient(TarantoolClientConfig config,
                                                            TarantoolClusterAddressProvider addressProvider,
                                                            int retries, long delay) {
        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(config, addressProvider);

        ProxyTarantoolTupleClient client = new ProxyTarantoolTupleClient(clusterClient);
        return new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies.byNumberOfAttempts(retries, e -> true).withDelay(delay).build());
    }

    @Test
    void testMultipleRoutersConnectWithUnreachable_retryableRequestShouldNotFail() throws Exception {
        // create retrying client with two routers, one does not exist
        TarantoolClusterAddressProvider addressProvider = () -> Arrays.asList(
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                new TarantoolServerAddress(container.getRouterHost(), 33399));

        RetryingTarantoolTupleClient client = setupClusterClient(prepareConfig().build(), addressProvider, 1, 0);

        assertDoesNotThrow(() -> client.call("reset_request_counters").get());
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());

        client.close();
    }

    @Test
    void testMultipleRoutersReconnect_retryableRequestShouldNotFail() throws Exception {
        RetryingTarantoolTupleClient routerClient1 = setupRouterClient(3301, 3, 10);
        routerClient1.call("reset_request_counters").get();
        RetryingTarantoolTupleClient routerClient2 = setupRouterClient(3311, 3, 10);
        routerClient2.call("reset_request_counters").get();

        // create retrying client with two routers
        TarantoolClusterAddressProvider addressProvider = () -> Arrays.asList(
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3311)));

        AtomicReference<Container.ExecResult> result = new AtomicReference<>();
        AtomicReference<String> nextRouterName = new AtomicReference<>();

        ClusterTarantoolTupleClient clusterClient =
                new ClusterTarantoolTupleClient(prepareConfig().withConnectTimeout(1000).build(), addressProvider);
        ProxyTarantoolTupleClient proxyClient = new ProxyTarantoolTupleClient(clusterClient);
        RetryingTarantoolTupleClient client = new RetryingTarantoolTupleClient(proxyClient,
                TarantoolRequestRetryPolicies.byNumberOfAttempts(10, e -> {
                    // after the first retry, we disconnect the router,
                    // which we retry, so that the request goes to another connection
                    if (e.getMessage().contains("Disabled by client")) {
                        try {
                            result.set(container.execInContainer(
                                    "cartridge", "stop", "--run-dir=/tmp/run", "--force", nextRouterName.get()));
                            assertEquals(0, result.get().getExitCode(), result.get().getStderr());
                        } catch (IOException | InterruptedException er) {
                            throw new RuntimeException(er);
                        }
                    }
                    return true;
                }).withDelay(0).build());

        // one of router accept request
        // get first router name in round robbin loop
        String firstRouterName = client.callForSingleResult("get_router_name", String.class).get();
        nextRouterName.set(firstRouterName.equals("router") ? "second-router" : "router");

        // the router with the passed name(nextRouterName.get()) will retry endlessly
        // after the first retry we will turn off this router
        // and it is expected that the request will go to another router
        assertTrue(client.callForSingleResult("long_running_function",
                        Collections.singletonList(Arrays.asList(0.5, nextRouterName.get())), Boolean.class).get());

        // start the turned off router to get requests
        result.set(container.execInContainer(
                "cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d", nextRouterName.get()));
        assertEquals(0, result.get().getExitCode(), result.get().getStderr());

        assertEquals(1, routerClient1.callForSingleResult("get_request_count", Integer.class).get());
        assertEquals(1, routerClient2.callForSingleResult("get_request_count", Integer.class).get());

        // full reconnection
        // this is necessary so that a client with two connections can see both routers
        result.set(container.execInContainer(
                "cartridge", "stop", "--run-dir=/tmp/run", "--force", "router"));
        assertEquals(0, result.get().getExitCode(), result.get().getStderr());
        result.set(container.execInContainer(
                "cartridge", "stop", "--run-dir=/tmp/run", "--force", "second-router"));
        assertEquals(0, result.get().getExitCode(), result.get().getStderr());
        result.set(container.execInContainer(
                "cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d", "router"));
        assertEquals(0, result.get().getExitCode(), result.get().getStderr());
        result.set(container.execInContainer(
                "cartridge", "start", "--run-dir=/tmp/run", "--data-dir=/tmp/data", "-d", "second-router"));
        assertEquals(0, result.get().getExitCode(), result.get().getStderr());

        // wait until full reconnection completed
        Unreliables.retryUntilTrue(5, TimeUnit.SECONDS, () -> {
            List<?> isHealthyResult =
                    routerClient1.eval("return require('cartridge').is_healthy()").get();
            if (isHealthyResult.get(0) == null) {
                // delay
                Thread.sleep(500);
                return false;
            }
            return (Boolean) isHealthyResult.get(0);
        });

        // initiate two requests
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());

        // check that they completed normally without reties and fell onto different routers (full reconnection effect)
        assertEquals(2, routerClient1.callForSingleResult("get_request_count", Integer.class).get());
        assertEquals(2, routerClient2.callForSingleResult("get_request_count", Integer.class).get());

        routerClient1.close();
        routerClient2.close();
        client.close();
    }

    @Test
    void testMultipleConnectionsReconnect_retryableRequestShouldNotFail() throws Exception {
        RetryingTarantoolTupleClient routerClient1 = setupRouterClient(3301, 3, 10);
        routerClient1.call("reset_request_counters").get();

        // create retrying client with one router and two connections
        TarantoolClusterAddressProvider addressProvider = () -> Collections.singletonList(
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)));
        RetryingTarantoolTupleClient client = setupClusterClient(
                prepareConfig().withConnections(2).build(), addressProvider, 10, 10);

        List<TarantoolConnection> connections = Collections.synchronizedList(new ArrayList<>());
        client.getConnectionListeners().add(conn -> {
            CustomConnection custom = new CustomConnection(conn);
            connections.add(custom);
            return CompletableFuture.completedFuture(custom);
        });
        // initialize connection
        client.getVersion();

        // initiate two long-running requests
        CompletableFuture<Boolean> request1 = client.callForSingleResult(
                "long_running_function", Collections.singletonList(0.5), Boolean.class);
        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(100);
                // close one connection
                connections.get(0).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        request1 = closeFuture.thenCombine(request1, (o, r) -> r);
        CompletableFuture<Boolean> request2 = client.callForSingleResult(
                "long_running_function", Collections.singletonList(0.5), Boolean.class);

        // both requests should return normally (alive connection selection effect)
        assertTrue(request1.get());
        assertTrue(request2.get());

        // get the request attempts -- there will be at least one retry
        assertTrue(routerClient1.callForSingleResult("get_request_count", Integer.class).get() >= 3);

        // initiate another long-running request
        request1 = client.callForSingleResult("long_running_function", Collections.singletonList(0.5), Boolean.class)
        .thenApply(r -> {
            try {
                // partial disconnect -> one connection is restored
                assertEquals(3, connections.size());

                // close remaining connections
                for (int i = 1; i < 3; i++) {
                    connections.get(i).close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return r;
        });

        // the request should return normally (reconnection effect)
        assertTrue(request1.get());

        // initiate next two requests and check that they completed normally
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());
        assertTrue(client.callForSingleResult("long_running_function", Boolean.class).get());

        // check that two connections restored (2 initial + 1 after partial dc + 2 after full dc)
        assertEquals(5, connections.size());
    }
}
