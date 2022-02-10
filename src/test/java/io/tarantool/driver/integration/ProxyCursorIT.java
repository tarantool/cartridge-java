package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProxyCursorIT extends SharedCartridgeContainer {

    private static final String TEST_SPACE_NAME = "cursor_test_space";
    private static final String TEST_MULTIPART_KEY_SPACE_NAME = "cursor_test_space_multi_part_key";
    private static final int DEFAULT_TIMEOUT = 5 * 1000;

    public static String USER_NAME;
    public static String PASSWORD;

    private static final Logger log = LoggerFactory.getLogger(ProxyCursorIT.class);

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
        initClient();

        try {
            truncateSpace(TEST_SPACE_NAME);
            insertToTestSpace(TEST_SPACE_NAME, 100,
                    i -> Arrays.asList(i, i + "abc", i * 10)
            );

            truncateSpace(TEST_MULTIPART_KEY_SPACE_NAME);
            insertToTestSpace(TEST_MULTIPART_KEY_SPACE_NAME, 50,
                    i -> Arrays.asList(i / 10, i + "abc", i * 10)
            );
        } catch (ExecutionException | InterruptedException ex) {
            log.error("Error: ", ex);
            fail();
        }
    }

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
    }

    public static void tearDown() throws Exception {
        client.close();
        assertThrows(TarantoolClientException.class, () -> client.metadata().getSpaceByName("_space"));
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
        client = TarantoolClientFactory.createClient()
                .withAddressProvider(getClusterAddressProvider())
                .withCredentials(USER_NAME, PASSWORD)
                .withConnectTimeout(DEFAULT_TIMEOUT)
                .withReadTimeout(DEFAULT_TIMEOUT)
                .withRequestTimeout(DEFAULT_TIMEOUT)
                .withProxyMethodMapping()
                .withRetryingByNumberOfAttempts(10,
                        TarantoolRequestRetryPolicies.retryTarantoolNoSuchProcedureErrors(),
                        b -> b.withDelay(100))
                .build();
    }

    private interface ElementGenerator {
        List<Object> generate(int index);
    }

    private static void insertToTestSpace(String spaceName,
                                          int count,
                                          ElementGenerator gen
    )
            throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations testSpace = client.space(spaceName);
        for (int i = 1; i <= count; i++) {
            testSpace.insert(
                    new TarantoolTupleImpl(
                            gen.generate(i), mapperFactory.defaultComplexTypesMapper()))
                    .get();
        }
    }

    @Test
    public void getOneTuple() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);

        Conditions conditions = Conditions.indexEquals("primary", Collections.singletonList(1));
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 10);

        assertTrue(cursor.next());
        TarantoolTuple tuple = cursor.get();

        assertEquals(1, tuple.getInteger(0));
        assertEquals("1abc", tuple.getString(1));
        assertEquals(10, tuple.getInteger(2));

        assertFalse(cursor.next());
        assertThrows(TarantoolSpaceOperationException.class, cursor::get);
    }

    @Test
    public void getTuples_withLimitAndCondition() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);

        Conditions conditions = Conditions
                .indexGreaterOrEquals("primary", Collections.singletonList(12))
                .withLimit(13);

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 3);

        assertTrue(cursor.next());
        List<Integer> tupleIds = new ArrayList<>();
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.get();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.next();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (hasNext);

        assertEquals(13, countTotal);
        assertEquals(Arrays.asList(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24), tupleIds);
        assertThrows(TarantoolSpaceOperationException.class, cursor::get);
    }

    @Test
    public void getTuplesWithLimitAndCondition_lessThat() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);

        Conditions conditions = Conditions
                .indexLessThan("primary", Collections.singletonList(53))
                .withLimit(14);

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 3);

        assertTrue(cursor.next());
        List<Integer> tupleIds = new ArrayList<>();
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.get();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.next();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (hasNext);

        assertEquals(14, countTotal);
        assertEquals(Arrays.asList(52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39), tupleIds);
        assertThrows(TarantoolSpaceOperationException.class, cursor::get);
    }

    @Test
    public void countAll_batchByOneElement() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 3);

        assertTrue(cursor.next());
        int countTotal = 0;

        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.get();
            hasNext = cursor.next();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (hasNext);

        assertEquals(100, countTotal);

        cursor = testSpace.cursor(conditions, 1);
        List<Integer> tupleIds = new ArrayList<>();
        assertTrue(cursor.next());
        countTotal = 0;
        do {
            countTotal++;
            TarantoolTuple t = cursor.get();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.next();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (hasNext);

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_smallBatch() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 10);

        assertTrue(cursor.next());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.get();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (cursor.next());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_batchSizeEqualCount() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 100);

        assertTrue(cursor.next());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.get();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (cursor.next());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_largeBatch() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 1000);

        assertTrue(cursor.next());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.get();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (cursor.next());

        assertEquals(100, countTotal);
    }

    @Test
    public void getTupleByPartialKey() {
        TarantoolSpaceOperations testSpace = client.space(TEST_MULTIPART_KEY_SPACE_NAME);

        Conditions conditions = Conditions.indexGreaterOrEquals("primary", Collections.singletonList(3));
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, 2);

        assertTrue(cursor.next());
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.get();
            hasNext = cursor.next();
            //protect from infinite loop
            assertTrue(countTotal <= 1000);
        } while (hasNext);

        assertEquals(21, countTotal);
        assertThrows(TarantoolSpaceOperationException.class, cursor::get);
    }
}
