package io.tarantool.driver.integration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.connection.TarantoolConnection;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.core.RetryingTarantoolTupleClient;

/**
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
@Testcontainers
public class ClusterConnectionIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    @BeforeAll
    public static void setUp() throws TimeoutException {
        startCluster();

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
                                                TarantoolRequestRetryPolicies.byNumberOfAttempts(retries)
                                                    .withDelay(delay).build());
    }

    private RetryingTarantoolTupleClient setupClusterClient(
        TarantoolClientConfig config,
        TarantoolClusterAddressProvider addressProvider,
        int retries, long delay) {
        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(config, addressProvider);

        ProxyTarantoolTupleClient client = new ProxyTarantoolTupleClient(clusterClient);
        return new RetryingTarantoolTupleClient(client,
                                                TarantoolRequestRetryPolicies.byNumberOfAttempts(retries, e -> true)
                                                    .withDelay(delay).build());
    }

    @Test
    void test_roundRobin_shouldWorkCorrectly_withDiscoveryAndConnections()
        throws ExecutionException, InterruptedException, IOException {

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clusterClient =
            getTarantoolClusterClientWithDiscovery(2, 5_000);

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient1 = getSimpleClient(3301);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient2 = getSimpleClient(3302);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient3 = getSimpleClient(3303);
        // 3306 isn't in cluster topology yet
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient4 = getSimpleClient(3306);

        int callCounter = 15;
        for (int i = 0; i < callCounter; i++) {
            clusterClient.callForSingleResult(
                "simple_long_running_function", Arrays.asList(0, true), Boolean.class).get();
        }

        String getAllConnectionCalls =
            "return box.space.request_counters.index.count:select(0, {iterator = box.index.GT})";
        // 15 calls on 3 routers on 2 connection == 15 / 3 == 5 / 2 == 2 or 3 calls per connect
        for (TarantoolClient router : Arrays.asList(routerClient1, routerClient2, routerClient3)) {
            assertEquals(Arrays.asList(2, 3), getCallCountersPerConnection(getAllConnectionCalls, router));
        }

        // add new router
        // put 3306 in topology as router
        routerClient1.eval("cartridge = require('cartridge') " +
            "replicasets = { " +
            "                { " +
            "                    alias = 'app-router-fourth', " +
            "                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles.api_router' }, " +
            "                    join_servers = { { uri = 'localhost:3306' } } " +
            "                }} " +
            "cartridge.admin_edit_topology({ replicasets = replicasets }) ").join();

        // wait until discovery get topology
        Thread.sleep(5_000);

        callCounter = 16;
        // 16 / 4 / 2 = 2 requests per connect
        for (int i = 0; i < callCounter; i++) {
            clusterClient.callForSingleResult(
                "simple_long_running_function", Arrays.asList(0, true), Boolean.class).get();
        }

        for (TarantoolClient router :
            Arrays.asList(routerClient1, routerClient2, routerClient3)) {
            assertEquals(Arrays.asList(4, 5), getCallCountersPerConnection(getAllConnectionCalls, router));
        }

        Object routerCallCounterPerConnection = getCallCountersPerConnection(getAllConnectionCalls, routerClient4);
        assertEquals(Arrays.asList(2, 2), routerCallCounterPerConnection);

        stopInstance("fourth-router");
        // wait until discovery get topology
        Thread.sleep(5_000);

        callCounter = 12;
        // 12 / 3 / 2 = 2 requests per connect
        for (int i = 0; i < callCounter; i++) {
            clusterClient.callForSingleResult(
                "simple_long_running_function", Arrays.asList(0, true), Boolean.class).get();
        }
        Thread.sleep(5_000);
        for (TarantoolClient router :
            Arrays.asList(routerClient1, routerClient2, routerClient3)) {
            assertEquals(Arrays.asList(6, 7), getCallCountersPerConnection(getAllConnectionCalls, router));
        }

        startCartridge();
    }

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getSimpleClient(Integer port) {
        return TarantoolClientFactory.createClient()
                .withAddress(container.getRouterHost(), container.getMappedPort(port))
                .withCredentials(USER_NAME, PASSWORD)
                .build();
    }

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>
    getTarantoolClusterClientWithDiscovery(
        int connections, int delay) {
        String host = container.getRouterHost();
        int port = container.getRouterPort();

        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            USER_NAME,
            PASSWORD
        );
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                                           .withCredentials(credentials)
                                           .build();

        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                                                      .withClientConfig(config)
                                                      .withEntryFunction("get_routers")
                                                      .withEndpointProvider(() -> Collections.singletonList(
                                                          new TarantoolServerAddress(
                                                              host, port
                                                          )))
                                                      .build();

        TarantoolClusterDiscoveryConfig clusterDiscoveryConfig = new TarantoolClusterDiscoveryConfig.Builder()
                                                                     .withDelay(delay)
                                                                     .withEndpoint(endpoint)
                                                                     .build();

        BinaryDiscoveryClusterAddressProvider discoveryProvider = new BinaryDiscoveryClusterAddressProvider(
            clusterDiscoveryConfig);

        TarantoolClusterAddressProvider wrapperDiscoveryProvider
            = new TestWrappedClusterAddressProvider(discoveryProvider, container); // because we use docker ports

        return TarantoolClientFactory.createClient()
                            .withAddressProvider(wrapperDiscoveryProvider)
                            .withCredentials(USER_NAME, PASSWORD)
                            .withConnections(connections)
                            .build();
    }

    @NotNull
    private static Object getCallCountersPerConnection(String getAllConnectionCalls, TarantoolClient router) {
        List<?> luaResponse = router.eval(getAllConnectionCalls).join();
        ArrayList tuples = (ArrayList) luaResponse.get(0); // because lua has multivalue response

        Object routerCallCounterPerConnection = tuples.stream()
                                                    .map(item -> ((ArrayList) item).get(1))
                                                    .collect(Collectors.toList());
        return routerCallCounterPerConnection;
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
        RetryingTarantoolTupleClient routerClient2 = setupRouterClient(3302, 3, 10);
        routerClient2.call("reset_request_counters").get();

        // create retrying client with two routers
        TarantoolClusterAddressProvider addressProvider = () -> Arrays.asList(
            new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
            new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3302)));

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
