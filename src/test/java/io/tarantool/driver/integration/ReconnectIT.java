package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.core.TarantoolDaemonThreadFactory;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.tarantool.driver.TarantoolUtils.retry;
import static io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.retryNetworkErrors;
import static io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.retryTarantoolNoSuchProcedureErrors;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Oleg Kuznetsov
 * @author Ivan Dneprov
 * <p>
 * WARNING: If you updated the code in this file, don't forget to update the MultiInstanceConnecting.md and
 * docs/RetryingTarantoolClient.md permalinks!
 */
@Testcontainers
public class ReconnectIT extends SharedCartridgeContainer {

    private static final Logger logger = LoggerFactory.getLogger(ReconnectIT.class);

    private static String USER_NAME;
    private static String PASSWORD;
    private static final String SPACE_NAME = "test__profile";
    private static final String PK_FIELD_NAME = "profile_id";
    private static AtomicInteger retryingCounter;

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

    @BeforeAll
    public static void setUp() throws TimeoutException {
        startCluster();

        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    @Test
    public void test_should_retryDelete_ifRecordsNotFound() throws InterruptedException {
        // Creating multiple clients
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> crudClient = getCrudClient();
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> retryingClient =
            getRetryingTarantoolClient(crudClient);

        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space =
            crudClient.space(SPACE_NAME);
        space.truncate().join();

        // Use TarantoolTupleFactory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(
            crudClient.getConfig().getMessagePackMapper());

        // Call delete_with_error_if_not_found before record was inserted
        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);
        CompletableFuture deleteFuture = retryingClient.space(SPACE_NAME).delete(conditions);
        retry(() -> assertEquals(1, retryingCounter.get()));
        space.insert(tupleFactory.create(1, null, "FIO", 50, 100)).join();
        deleteFuture.join();
        TarantoolResult<TarantoolTuple> selectResult = space.select(conditions).join();
        assertEquals(0, selectResult.size());
    }

    /**
     * Checking if this test is valid is here
     * {@link TarantoolErrorsIT#test_should_throwTarantoolNoSuchProcedureException_ifProcedureIsNil}
     */
    @Test
    public void test_should_reconnect_ifCrudProcedureIsNotDefined() {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> crudClient = getCrudClient();
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> retryingClient =
            getRetryingTarantoolClient(crudClient);

        try {
            //save procedure to tmp variable set it to nil and call
            crudClient.eval("rawset(_G, 'tmp_test_no_such_procedure', test_no_such_procedure)")
                .thenAccept(c -> crudClient.eval("rawset(_G, 'test_no_such_procedure', nil)"))
                .thenApply(c -> crudClient.call("test_no_such_procedure"))
                .join();
        } catch (CompletionException exception) {
            assertTrue(exception.getCause() instanceof TarantoolNoSuchProcedureException);
        }

        //start another thread that will return the procedure back after 100 ms
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            crudClient.eval("rawset(_G, 'test_no_such_procedure', tmp_test_no_such_procedure)").join();
        }).start();

        assertDoesNotThrow(() -> retryingClient.call("test_no_such_procedure").join());
        assertEquals("test_no_such_procedure", retryingClient.call("test_no_such_procedure").join().get(0));
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getCrudClient() {
        return TarantoolClientFactory.createClient()
            // You can connect to multiple routers
            .withAddresses(
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.routerPort)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.router2Port)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.router3Port))
            )
            // For connecting to a Cartridge application,
            // use the value of cluster_cookie parameter in the init.lua file
            .withCredentials(USER_NAME, PASSWORD)
            // Number of connections per Tarantool instance
            .withConnections(10)
            // Specify using the default CRUD proxy operations mapping configuration
            .withProxyMethodMapping()
            .build();
    }

    @Test
    public void test_should_reconnect_ifReconnectIsInvoked() throws Exception {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
            getRetryingTarantoolClient(getCrudClient());

        // getting all routers uuids
        final Set<String> routerUuids = getInstancesUuids(client);

        // stop routers
        stopInstance("router", true);
        stopInstance("second-router", true);

        // check that there is only one instance left
        assertEquals(getInstanceUuid(client), getInstanceUuid(client));

        // start routers
        startCartridge();

        client.refresh();

        Set<String> uuidsAfterReconnect = null;
        int attempts = 10;
        int sleepMs = 3000;
        while (attempts > 0) {

            // getting all routers uuids after restarting
            uuidsAfterReconnect = getInstancesUuids(client);
            // check that amount of routers is equal initial amount
            if (routerUuids.size() == uuidsAfterReconnect.size()) {
                break;
            } else {
                logger.info("Not all routers are online, sleeping {} ms", sleepMs);
            }

            Thread.sleep(sleepMs);
            attempts--;
        }
        assertEquals(routerUuids.size(), uuidsAfterReconnect.size());
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getRetryingTarantoolClient(
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {

        retryingCounter = new AtomicInteger();

        return TarantoolClientFactory.configureClient(client)
            // Configure a custom delete function
            .withProxyMethodMapping(builder -> builder.withDeleteFunctionName("delete_with_error_if_not_found"))
            // Set retrying policy
            // First parameter is number of attempts
            .withRetryingByNumberOfAttempts(5,
                // You can use default predicates from TarantoolRequestRetryPolicies for checking errors
                retryNetworkErrors()
                    // Also you can use your own predicates and combine them with each other
                    .or(e -> e.getMessage().contains("Unsuccessful attempt"))
                    .or(e -> {
                        retryingCounter.getAndIncrement();
                        return e.getMessage().contains("Records not found");
                    })
                    // Or with defaults
                    .or(retryTarantoolNoSuchProcedureErrors()),
                // Also you can set delay in millisecond between attempts
                factory -> factory.withDelay(300)
            )
            .build();
    }

    @Test
    public void
    test_should_removeUnavailableHostsFromAddressProvider_ifDiscoveryProcedureReturnStatusNotHealthyAndNotAvailable()
        throws Exception {
        initRouterStatuses();
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = initClientWithDiscovery();

        final Set<String> instancesUuids = getInstancesUuids(client);
        assertEquals(3, instancesUuids.size());

        replaceInstancesInfo(client, 1, "unavailable", super.routerPort);
        replaceInstancesInfo(client, 2, "unavailable", super.router2Port);
        Thread.sleep(1000);

        final Set<String> afterRoutersDisablingInstancesUuids = getInstancesUuids(client);
        assertEquals(1, afterRoutersDisablingInstancesUuids.size());

        replaceInstancesInfo(client, 1, "available", super.routerPort);
        replaceInstancesInfo(client, 2, "available", super.router2Port);
        Thread.sleep(1000);

        final Set<String> afterRoutersEnablingInstancesUuids = getInstancesUuids(client);
        assertEquals(3, afterRoutersEnablingInstancesUuids.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void test_should_closeConnections_ifAddressProviderReturnsNewAddresses() throws Exception {
        // restart routers for resetting connections
        stopInstances(Arrays.asList("router", "second-router"));
        startCartridge();

        final TarantoolServerAddress firstAddress =
            new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.routerPort));
        final TarantoolServerAddress secondAddress =
            new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.router2Port));
        final TarantoolServerAddress[] tarantoolServerAddresses = {firstAddress, secondAddress};

        // create client for check number of connections
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clientForCheck =
            TarantoolClientFactory.createClient()
                .withCredentials(container.getUsername(), container.getPassword())
                .withAddresses(tarantoolServerAddresses)
                .withRetryingByNumberOfAttempts(10, b -> b.withDelay(100))
                .build();
        // reset connection statistics
        clientForCheck.eval("return box.stat.reset()").join();

        // make a function which returns everytime a one of two addresses in order
        AtomicBoolean isNextAddress = new AtomicBoolean(false);
        Callable<List<TarantoolServerAddress>> getTarantoolServerAddresses = () -> {
            if (isNextAddress.compareAndSet(true, false)) {
                return Collections.singletonList(secondAddress);
            }
            isNextAddress.compareAndSet(false, true);
            return Collections.singletonList(firstAddress);
        };

        // make client for testing
        final AtomicBoolean isRefreshNeeded = new AtomicBoolean(false);
        final AtomicInteger numberOfSwitching = new AtomicInteger(0);
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
            TarantoolClientFactory.createClient()
                .withCredentials(container.getUsername(), container.getPassword())
                .withAddressProvider(new TarantoolClusterAddressProvider() {

                    @Override
                    public Collection<TarantoolServerAddress> getAddresses() {
                        try {
                            return getTarantoolServerAddresses.call();
                        } catch (Exception exception) {
                            exception.printStackTrace();
                            return Collections.emptyList();
                        }
                    }

                    @Override
                    public void setRefreshCallback(Runnable runnable) {
                        // make a daemon that will refresh connections every 100ms
                        Executors.newSingleThreadScheduledExecutor(
                            new TarantoolDaemonThreadFactory("refresh-connections")
                        ).scheduleAtFixedRate(() -> {
                            if (isRefreshNeeded.get()) {
                                numberOfSwitching.incrementAndGet();
                                runnable.run();
                            }
                        }, 500, 100, TimeUnit.MILLISECONDS);
                    }
                }).build();

        //initiate establish connection
        client.getVersion();

        isRefreshNeeded.set(true);
        logger.info("Waiting while number of switching won't be 50");
        while (numberOfSwitching.get() < 50) {
            Thread.sleep(100);
        }

        final List<Map<String, Map<String, Integer>>> result = (List<Map<String, Map<String, Integer>>>)
            clientForCheck.eval("return box.stat.net()").join();

        assertTrue(result.get(0).get("CONNECTIONS").get("current") < 20);
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> initClientWithDiscovery() {
        final BinaryClusterDiscoveryEndpoint discoveryEndpoint = BinaryClusterDiscoveryEndpoint.builder()
            .withEntryFunction("get_routers_status")
            .withEndpointProvider(this::getShuffledTarantoolServerAddresses)
            .withClientConfig(TarantoolClientConfig.builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .build()
            )
            .build();

        final BinaryDiscoveryClusterAddressProvider binaryDiscoveryClusterAddressProvider =
            new BinaryDiscoveryClusterAddressProvider(TarantoolClusterDiscoveryConfig.builder()
                .withEndpoint(discoveryEndpoint)
                .withDelay(50)
                .build());

        return TarantoolClientFactory.createClient()
            .withAddressProvider(
                new TestWrappedClusterAddressProvider(binaryDiscoveryClusterAddressProvider, container))
            .withCredentials(USER_NAME, PASSWORD)
            .withConnections(10)
            .withProxyMethodMapping()
            .build();
    }

    private void initRouterStatuses() {
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> initClient =
            TarantoolClientFactory.createClient()
                // You can connect to multiple routers
                // Do not forget to shuffle your addresses if you are using multiple clients
                .withAddresses(getShuffledTarantoolServerAddresses())
                // For connecting to a Cartridge application,
                // use the value of cluster_cookie parameter in the init.lua file
                .withCredentials(USER_NAME, PASSWORD)
                // Number of connections per Tarantool instance
                .withConnections(1)
                // Specify using the default CRUD proxy operations mapping configuration
                .withProxyMethodMapping()
                .build();
        List<String> routerURIs = Arrays.asList(
            String.format("%s:%s", "localhost", super.routerPort),
            String.format("%s:%s", "localhost", super.router2Port),
            String.format("%s:%s", "localhost", super.router3Port)
        );
        initClient.call("init_router_status", Arrays.asList(routerURIs)).join();
    }

    private void replaceInstancesInfo(
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client,
        int id, String status, Integer port) {
        final TarantoolTuple tuple = tupleFactory
            .create(id, 1, UUID.randomUUID().toString(), status, String.format("%s:%s", "localhost", port));

        client.space("instances_info").replace(tuple).join();
    }

    @NotNull
    private List<TarantoolServerAddress> getShuffledTarantoolServerAddresses() {
        List<TarantoolServerAddress> addresses = Arrays.asList(
            new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.routerPort)),
            new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.router2Port)),
            new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(super.router3Port))
        );
        Collections.shuffle(addresses);

        return addresses;
    }

    /**
     * Return all instances uuids from cluster, using round robin connection selection strategy
     *
     * @param client Tarantool client
     * @return set of instances uuids from cluster
     */
    private Set<String> getInstancesUuids(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        String firstUuid = getInstanceUuid(client);

        final Set<String> instancesUuids = new HashSet<>();
        instancesUuids.add(firstUuid);

        String currentUuid = "";
        while (!firstUuid.equals(currentUuid)) {
            currentUuid = getInstanceUuid(client);
            instancesUuids.add(currentUuid);
        }

        return instancesUuids;
    }

    private String getInstanceUuid(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return (String) client.eval("return box.info().uuid").join().get(0);
    }
}
