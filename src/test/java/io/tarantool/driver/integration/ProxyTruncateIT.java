package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
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
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.core.RetryingTarantoolTupleClient;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.tarantool.driver.integration.Utils.checkSpaceIsEmpty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Ivan Dneprov
 */
public class ProxyTruncateIT extends SharedCartridgeContainer {

    private static RetryingTarantoolTupleClient client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
            new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

    public static String USER_NAME;
    public static String PASSWORD;

    private static final String TEST_SPACE_NAME = "test__profile";
    private static final int DEFAULT_TIMEOUT = 5 * 1000;

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
        initClient();
    }

    @BeforeEach
    public void truncateSpace() {
        client.space(TEST_SPACE_NAME).truncate().join();
    }

    private static TarantoolClusterAddressProvider getClusterAddressProvider() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(credentials)
                .withReadTimeout(DEFAULT_TIMEOUT)
                .withConnectTimeout(DEFAULT_TIMEOUT)
                .build();

        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                .withClientConfig(config)
                .withEntryFunction("get_routers")
                .withEndpointProvider(() -> Collections.singletonList(
                        new TarantoolServerAddress(container.getRouterHost(), container.getRouterPort())))
                .build();

        TarantoolClusterDiscoveryConfig clusterDiscoveryConfig = new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withDelay(1)
                .build();

        return new TestWrappedClusterAddressProvider(
                new BinaryDiscoveryClusterAddressProvider(clusterDiscoveryConfig),
                container);
    }

    public static void initClient() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(DEFAULT_TIMEOUT)
                .withReadTimeout(DEFAULT_TIMEOUT)
                .withRequestTimeout(DEFAULT_TIMEOUT)
                .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
                config, getClusterAddressProvider());
        client = new RetryingTarantoolTupleClient(new ProxyTarantoolTupleClient(clusterClient),
                TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
                        .builder(10, thr -> thr instanceof TarantoolNoSuchProcedureException)
                        .withDelay(100)
                        .build());
    }

    @Test
    public void test_truncate2TimesOneSpace_shouldNotThrowExceptionsAndSpaceShouldBeEmptyAfterEachCall() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
                client.space(TEST_SPACE_NAME);

        // call truncate then space is empty
        testSpace.truncate().join();
        checkSpaceIsEmpty(testSpace);

        for (int i = 0; i < 2; i++) {
            // prepare values to insert
            TarantoolTuple tarantoolTuple = tupleFactory.create(1_000_000, null, "FIO", 50, 100);

            // then insert prepared values
            testSpace.insert(tarantoolTuple).join();

            // when values are inserted check that space isn't empty
            TarantoolResult<TarantoolTuple> selectResult = testSpace.select(Conditions.any()).join();
            assertEquals(1, selectResult.size());

            // after that truncate space
            testSpace.truncate().join();
            checkSpaceIsEmpty(testSpace);
        }
    }

    @Test
    public void test_truncateEmptySpace_shouldNotThrowExceptionsAndSpaceShouldBeEmpty() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
                client.space(TEST_SPACE_NAME);

        // truncate space to make sure it is empty
        testSpace.truncate().join();

        // when truncate empty space and check that now exceptions was thrown and space is empty
        assertDoesNotThrow(() -> testSpace.truncate().join());
        checkSpaceIsEmpty(testSpace);
    }
}
