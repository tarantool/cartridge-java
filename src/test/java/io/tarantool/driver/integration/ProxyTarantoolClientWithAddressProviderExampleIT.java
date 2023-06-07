package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ProxyTarantoolClientWithAddressProviderExampleIT extends SharedCartridgeContainer {

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    public static TarantoolCredentials credentials;

    private static final String SPACE_NAME = "test__profile";

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        initClient();
        truncateSpace(SPACE_NAME);
    }

    private static TarantoolClusterAddressProvider getClusterShuffledAddressProvider() {
        TarantoolClientConfig config = TarantoolClientConfig.builder()
            .withCredentials(credentials)
            .build();

        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
            // Config for connecting to instance with entry function
            .withClientConfig(config)
            // Name of a function that returns a pool of addresses to connect to
            .withEntryFunction("get_routers")
            // Setting first router URI as entry point
            .withEndpointProvider(() -> {
                List<TarantoolServerAddress> addresses = Collections.singletonList(
                    new TarantoolServerAddress(container.getRouterHost(), container.getRouterPort()));
                // Shuffling addresses in address provider
                Collections.shuffle(addresses);
                return addresses;
            })
            .build();

        TarantoolClusterDiscoveryConfig clusterDiscoveryConfig = new TarantoolClusterDiscoveryConfig.Builder()
            .withEndpoint(endpoint)
            .withDelay(1)
            .build();

        // TestWrappedClusterAddressProvider changes ports provided by address providers to docker mapped ports
        // You need to use TestWrappedClusterAddressProvider only if you run Tarantool in TestContainers
        return new TestWrappedClusterAddressProvider(
            new BinaryDiscoveryClusterAddressProvider(clusterDiscoveryConfig),
            container);
    }

    public static void initClient() {
        // For connecting to a Cartridge application, use the value of cluster_cookie parameter in the init.lua file
        credentials = new SimpleTarantoolCredentials(container.getUsername(),
            container.getPassword());

        client = TarantoolClientFactory.createClient()
            // You don't have to set the routers addresses yourself address provider will do it for you
            // Do not forget to shuffle your addresses if you are using multiple clients
            .withAddressProvider(getClusterShuffledAddressProvider())
            // For connecting to a Cartridge application, use the value of cluster_cookie parameter in the init.lua file
            .withCredentials(credentials)
            // Specify using the default CRUD proxy operations mapping configuration
            .withProxyMethodMapping()
            .build();
    }

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
    }

    @Test
    public void insertOneTuple() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space = client.space(SPACE_NAME);

        // Use TarantoolTupleFactory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(
            client.getConfig().getMessagePackMapper());

        TarantoolResult<TarantoolTuple> insertTuples = space.insert(tupleFactory.create(1, null, "FIO", 50, 100)).get();

        assertEquals(insertTuples.size(), 1);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 1);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "FIO");
        assertEquals(tuple.getInteger(3), 50);
        assertEquals(tuple.getInteger(4), 100);
    }
}
