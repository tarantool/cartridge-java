package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.DefaultTarantoolTupleFactory;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolTupleFactory;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolIndexType;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Alexey Andreev
 */
public class ProxyTarantoolClientMixedInstancesIT extends SharedCartridgeContainerMixedInstances {

    private static ProxyTarantoolTupleClient client;
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
        truncateSpace(TEST_SPACE_NAME);
    }

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
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
        client = new ProxyTarantoolTupleClient(clusterClient);
    }

    @Test
    public void getClusterMetaDataTest() {
        TarantoolMetadataOperations metadataOperations = client.metadata();

        Optional<TarantoolSpaceMetadata> spaceMeta = metadataOperations.getSpaceByName(TEST_SPACE_NAME);
        assertTrue(spaceMeta.isPresent());
        TarantoolSpaceMetadata spaceMetadata = spaceMeta.get();
        assertEquals(TEST_SPACE_NAME, spaceMetadata.getSpaceName());
        assertEquals(-1, spaceMetadata.getOwnerId());
        // FIXME Blocked by https://github.com/tarantool/ddl/issues/52
//        assertTrue(spaceMetadata.getSpaceId() > 0);
        assertEquals(5, spaceMetadata.getSpaceFormatMetadata().size());
        assertEquals(3, spaceMetadata.getFieldPositionByName("age"));
        assertEquals("unsigned", spaceMetadata.getFieldByName("age").get().getFieldType());

        Optional<TarantoolIndexMetadata> indexMeta = metadataOperations
                .getIndexByName(TEST_SPACE_NAME, "bucket_id");
        assertTrue(indexMeta.isPresent());
        TarantoolIndexMetadata indexMetadata = indexMeta.get();
        assertEquals("bucket_id", indexMetadata.getIndexName());
        assertEquals(1, indexMetadata.getIndexId());
        assertEquals(TarantoolIndexType.TREE, indexMetadata.getIndexType());
        assertFalse(indexMetadata.getIndexOptions().isUnique());
    }

    @Test
    public void clusterInsertSelectTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple;

        List<CompletableFuture<?>> allFutures = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            tarantoolTuple = tupleFactory.create(1_000_000 + i, null, "FIO", 50 + i, 100 + i);
            allFutures.add(profileSpace.insert(tarantoolTuple));
        }
        allFutures.forEach(CompletableFuture::join);

        Conditions conditions = Conditions.greaterOrEquals("profile_id", 1_000_000);

        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(conditions).get();
        assertEquals(20, selectResult.size());

        TarantoolTuple tuple;
        for (int i = 0; i < 20; i++) {
            tuple = selectResult.get(i);
            assertEquals(5, tuple.size());
            assertEquals(1_000_000 + i, tuple.getInteger(0));
            assertNotNull(tuple.getInteger(1)); //bucket_id
            assertEquals("FIO", tuple.getString(2));
            assertEquals(50 + i, tuple.getInteger(3));
            assertEquals(100 + i, tuple.getInteger(4));
        }

        conditions = conditions.startAfter(tupleFactory.create(1_000_000 + 9, null, "FIO", 59, 109));
        selectResult = profileSpace.select(conditions).get();
        assertEquals(10, selectResult.size());
    }

    @Test
    public void clusterInsertDeleteTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(100, null, "fio", 10, 100);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(insertTuples.size(), 1);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 100);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "fio");

        values = Arrays.asList(2, null, "John Doe", 55, 99);
        tarantoolTuple = tupleFactory.create(values);
        insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(1, insertTuples.size());
        tuple = insertTuples.get(0);
        assertEquals(5, tuple.size());
        assertEquals(2, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals("John Doe", tuple.getString(2));

        Conditions conditions = Conditions.equals(0, 2);
        TarantoolResult<TarantoolTuple> deleteResult = profileSpace.delete(conditions).get();

        assertEquals(1, deleteResult.size());
        tuple = deleteResult.get(0);
        assertEquals(5, tuple.size());
        assertEquals(2, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals("John Doe", tuple.getString(2));
    }

    @Test
    public void replaceTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(123, null, "Jane Doe", 18, 999);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(insertTuples.size(), 1);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 123);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "Jane Doe");
        assertEquals(tuple.getInteger(3), 18);
        assertEquals(tuple.getInteger(4), 999);

        List<Object> replaceValues = Arrays.asList(123, null, "John Doe", 21, 100);
        tarantoolTuple = tupleFactory.create(replaceValues);
        TarantoolResult<TarantoolTuple> replaceResult = profileSpace.replace(tarantoolTuple).get();
        assertEquals(replaceResult.size(), 1);
        tuple = replaceResult.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 123);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "John Doe");
        assertEquals(tuple.getInteger(3), 21);
        assertEquals(tuple.getInteger(4), 100);
    }

    @Test
    public void clusterUpdateTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(223, null, "Jane Doe", 10, 10);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(insertTuples.size(), 1);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 223);
        assertNotNull(tuple.getInteger(1)); //bucket_id

        Conditions conditions = Conditions.indexEquals("profile_id", Collections.singletonList(223));

        TarantoolResult<TarantoolTuple> updateResult;

        updateResult = profileSpace.update(conditions, TupleOperations.add(3, 90).andAdd(4, 5)).get();
        assertEquals(223, updateResult.get(0).getInteger(0));
        assertEquals(100, updateResult.get(0).getInteger(3));
        assertEquals(15, updateResult.get(0).getInteger(4));
    }

    @Test
    public void clusterUpsertTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(301, null, "Jack Sparrow", 30, 0);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);

        Conditions conditions = Conditions.equals("profile_id", 301);

        TarantoolResult<TarantoolTuple> upsertResult;

        //first time tuple not exist
        profileSpace.upsert(conditions,
                tarantoolTuple, TupleOperations.add(3, 5).andBitwiseOr(4, 7)).get();

        upsertResult = profileSpace.select(conditions).get();
        assertEquals(1, upsertResult.size());
        TarantoolTuple tuple = upsertResult.get(0);
        assertEquals(5, tuple.size());
        assertEquals(301, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(30, upsertResult.get(0).getInteger(3));
        assertEquals(0, upsertResult.get(0).getInteger(4));

        //second time tuple exist
        profileSpace.upsert(conditions,
                tarantoolTuple, TupleOperations.add(3, 5).andBitwiseOr(4, 7)).get();

        upsertResult = profileSpace.select(conditions).get();
        tuple = upsertResult.get(0);
        assertEquals(tuple.getInteger(0), 301);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(35, upsertResult.get(0).getInteger(3));
        assertEquals(7, upsertResult.get(0).getInteger(4));
    }

    @Test
    public void functionAggregateResultViaCallTest() throws ExecutionException, InterruptedException {
        List<Object> values = Arrays.asList(123000, null, "Jane Doe", 999);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);
        client.space("test_space").insert(tarantoolTuple).get();

        values = Arrays.asList(123000, null, true, 123.456);
        tarantoolTuple = tupleFactory.create(values);
        client.space("test_space_to_join").insert(tarantoolTuple).get();

        MessagePackValueMapper valueMapper = client.getConfig().getMessagePackMapper();
        CallResultMapper<TestComposite, SingleValueCallResult<TestComposite>> mapper =
                client.getResultMapperFactoryFactory().<TestComposite>singleValueResultMapperFactory()
                        .withSingleValueResultConverter(v -> {
                            Map<String, Object> valueMap = valueMapper.fromValue(v);
                            TestComposite composite = new TestComposite();
                            composite.field1 = (String) valueMap.get("field1");
                            composite.field2 = (Integer) valueMap.get("field2");
                            composite.field3 = (Boolean) valueMap.get("field3");
                            composite.field4 = (Double) valueMap.get("field4");
                            return composite;
                        }, TestCompositeCallResult.class);
        TestComposite actual =
                client.callForSingleResult("get_composite_data", Collections.singletonList(123000), mapper).get();

        assertEquals("Jane Doe", actual.field1);
        assertEquals(999, actual.field2);
        assertEquals(true, actual.field3);
        assertEquals(123.456, actual.field4);
    }
}
