package io.tarantool.driver.integration;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tarantool.driver.TarantoolUtils;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
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

    // TODO: Add parallel threads
    // TODO: A lot of routers
    // TODO: Parallel round robin and default as test parameter
    @Test
    void test_roundRobin_shouldWorkCorrectly_withDiscoveryAndConnections()
        throws ExecutionException, InterruptedException {

        TarantoolCartridgeContainer testContainer = runIndependentContainer();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clusterClient =
            getTarantoolClusterClientWithDiscovery(testContainer, 2, 2_000);

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient1 = getSimpleClient(testContainer,
            3301);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient2 = getSimpleClient(testContainer,
            3302);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient3 = getSimpleClient(testContainer,
            3303);

        int batch = 1500;
        int batchPerConnect =
            batch / 3 / 2; // 1_500 calls on 3 routers on 2 connection == 1_500 / 6 == 250 calls per connect
        for (int i = 0; i < batch; i++) {
            clusterClient.callForSingleResult(
                "simple_long_running_function", Arrays.asList(0, true), Boolean.class).get();
        }

        for (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> router : Arrays.asList(routerClient1,
            routerClient2,
            routerClient3)) {
            assertEquals(Arrays.asList(batchPerConnect, batchPerConnect), getCallCountersPerConnection(router));
        }
    }

    @Test
    void test_roundRobin_shouldWorkCorrectly_withDiscoveryAndConnections_andRouterJoining()
        throws ExecutionException, InterruptedException {

        TarantoolCartridgeContainer testContainer = runIndependentContainer();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clusterClient =
            getTarantoolClusterClientWithDiscovery(testContainer, 2, 2_000);

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient1 = getSimpleClient(testContainer,
            3301);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient2 = getSimpleClient(testContainer,
            3302);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient3 = getSimpleClient(testContainer,
            3303);
        // 3306 is not in cluster topology yet
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient4 = getSimpleClient(testContainer,
            3306);

        // add new router
        // put 3306 in topology as router
        routerClient1.eval("cartridge = require('cartridge') " +
                               "replicasets = { " +
                               "                { " +
                               "                    alias = 'app-router-fourth', " +
                               "                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles" +
                               ".api_router' }, " +
                               "                    join_servers = { { uri = 'localhost:3306' } } " +
                               "                }} " +
                               "cartridge.admin_edit_topology({ replicasets = replicasets }) ").join();

        int batch = 2000;
        int batchPerConnect =
            batch / 4 / 2; // 2_000 calls on 4 routers on 2 connection == 2_000 / 8 == 250 calls per connect
        for (int i = 0; i < batch; i++) {
            clusterClient.callForSingleResult(
                "simple_long_running_function", Arrays.asList(0, true), Boolean.class).get();
        }

        AtomicInteger sum = new AtomicInteger();
        for (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> router :
            Arrays.asList(routerClient1, routerClient2, routerClient3)) {
            assertEquals(2, getCallCountersPerConnection(router).stream().filter(cnt -> {
                sum.addAndGet(cnt);
                return cnt > batchPerConnect; // because forth router was in starting stage some time
            }).count());
        }
        assertEquals(2, getCallCountersPerConnection(routerClient4).stream().filter(cnt -> {
            sum.addAndGet(cnt);
            return 0 < cnt && cnt < batchPerConnect;
        }).count());
        assertEquals(batch, sum.get());
    }

    @Test
    void test_roundRobin_shouldWorkCorrectly_withDiscoveryAndConnections_andRouterDying()
        throws ExecutionException, InterruptedException, IOException {

        TarantoolCartridgeContainer testContainer = runIndependentContainer();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clusterClient =
            getTarantoolClusterClientWithDiscovery(testContainer, 2, 2_000);

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient1 = getSimpleClient(testContainer,
            3301);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient2 = getSimpleClient(testContainer,
            3302);
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient3 = getSimpleClient(testContainer,
            3303);
        // 3306 is not in cluster topology yet
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> routerClient4 = getSimpleClient(testContainer,
            3306);

        // add new router
        // put 3306 in topology as router
        routerClient1.eval("cartridge = require('cartridge') " +
                               "replicasets = { " +
                               "                { " +
                               "                    alias = 'app-router-fourth', " +
                               "                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles" +
                               ".api_router' }, " +
                               "                    join_servers = { { uri = 'localhost:3306' } } " +
                               "                }} " +
                               "cartridge.admin_edit_topology({ replicasets = replicasets }) ").join();

        String healthyCmd = "return cartridge.is_healthy()";

        TarantoolUtils.retry(() -> {
            try {
                assertEquals(true, testContainer.executeCommand(healthyCmd).get().get(0));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        stopInstance("fourth-router");

        int batch = 2000;
        int batchPerConnect =
            batch / 4 / 2; // 2_000 calls on 4 routers on 2 connection == 2_000 / 8 == 250 calls per connect
        for (int i = 0; i < batch; i++) {
            clusterClient.callForSingleResult(
                "simple_long_running_function", Arrays.asList(0, true), Boolean.class).get();
        }

        AtomicInteger sum = new AtomicInteger();
        for (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> router :
            Arrays.asList(routerClient1, routerClient2, routerClient3)) {
            assertEquals(2, getCallCountersPerConnection(router).stream().filter(cnt -> {
                sum.addAndGet(cnt);
                return cnt > batchPerConnect;
            }).count());
        }
        assertEquals(2, getCallCountersPerConnection(routerClient4).stream().filter(cnt -> {
            sum.addAndGet(cnt);
            return 0 < cnt && cnt < batchPerConnect;
        }).count());
        assertEquals(batch, sum.get());
    }

    private static TarantoolCartridgeContainer runIndependentContainer() {
        TarantoolCartridgeContainer container =
            new TarantoolCartridgeContainer(
                "cartridge/instances.yml",
                "cartridge/topology.lua")
                .withDirectoryBinding("cartridge")
                .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(ClusterConnectionIT.class)))
                .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 4))
                .withStartupTimeout(Duration.ofMinutes(2));
        container.start();
        return container;
    }

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getSimpleClient(
        TarantoolCartridgeContainer testContainer, Integer port) {
        return TarantoolClientFactory.createClient()
                   .withAddress(testContainer.getRouterHost(), testContainer.getMappedPort(port))
                   .withCredentials(testContainer.getUsername(), testContainer.getPassword())
                   .build();
    }

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>
    getTarantoolClusterClientWithDiscovery(TarantoolCartridgeContainer testContainer,
        int connections, int delay) {
        String host = testContainer.getRouterHost();
        int port = testContainer.getRouterPort();
        String username = testContainer.getUsername();
        String password = testContainer.getPassword();

        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            username,
            password
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
            = new TestWrappedClusterAddressProvider(discoveryProvider, testContainer); // because we use docker ports

        return TarantoolClientFactory.createClient()
                   .withAddressProvider(wrapperDiscoveryProvider)
                   .withCredentials(username, password)
                   .withConnections(connections)
                   .build();
    }

    @NotNull
    private static List<Integer> getCallCountersPerConnection(
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> router)
        throws ExecutionException, InterruptedException {
        TarantoolResult<TarantoolTuple> tuples = router.space("request_counters")
                                                     .select(
                                                         Conditions.indexGreaterThan(
                                                             "count",
                                                             Collections.singletonList(0))).get();
        return tuples.stream()
                   .map(tuple -> tuple.getInteger("count"))
                   .collect(Collectors.toList());
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
