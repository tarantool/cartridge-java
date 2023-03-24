package io.tarantool.driver.integration;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexType;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.core.RetryingTarantoolTupleClient;
import io.tarantool.driver.core.TarantoolTupleResultImpl;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactory;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactoryImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public class ProxyTarantoolClientIT extends SharedCartridgeContainer {

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private static final MessagePackMapper defaultMapper =
        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(defaultMapper);
    private static final ResultMapperFactoryFactoryImpl mapperFactoryFactory =
        new ResultMapperFactoryFactoryImpl();

    public static String USER_NAME;
    public static String PASSWORD;

    private static final String TEST_SPACE_NAME = "test__profile";
    private static final String PK_FIELD_NAME = "profile_id";
    private static final String BUCKET_ID_FIELD_NAME = "bucket_id";
    private static final int DEFAULT_TIMEOUT = 5 * 1000;

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
        initClient();
        truncateSpace(TEST_SPACE_NAME);
    }

    @BeforeEach
    public void truncateSpace() {
        truncateSpace("test_space");
        truncateSpace("test_space_to_join");
        truncateSpace("second_test_space");
        truncateSpace("cursor_test_space");
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

        client = new RetryingTarantoolTupleClient(new ProxyTarantoolTupleClient(clusterClient),
            TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
                .builder(10, thr -> thr instanceof TarantoolNoSuchProcedureException)
                .withDelay(100)
                .build());
    }

    @Test
    public void getClusterMetaDataTest() {
        TarantoolMetadataOperations metadataOperations = client.metadata();

        Optional<TarantoolSpaceMetadata> spaceMeta = metadataOperations.getSpaceByName(TEST_SPACE_NAME);
        assertTrue(spaceMeta.isPresent());
        TarantoolSpaceMetadata spaceMetadata = spaceMeta.get();
        assertEquals(TEST_SPACE_NAME, spaceMetadata.getSpaceName());
        assertEquals(-1, spaceMetadata.getOwnerId());
        assertEquals(5, spaceMetadata.getSpaceFormatMetadata().size());
        assertEquals(3, spaceMetadata.getFieldPositionByName("age"));
        assertEquals("unsigned", spaceMetadata.getFieldByName("age").get().getFieldType());

        Optional<TarantoolIndexMetadata> indexMeta = metadataOperations
            .getIndexByName(TEST_SPACE_NAME, BUCKET_ID_FIELD_NAME);
        assertTrue(indexMeta.isPresent());
        TarantoolIndexMetadata indexMetadata = indexMeta.get();
        assertEquals(BUCKET_ID_FIELD_NAME, indexMetadata.getIndexName());
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

        Conditions conditions = Conditions.greaterOrEquals(PK_FIELD_NAME, 1_000_000);

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

        conditions.startAfter(tupleFactory.create(1_000_000 + 9, null, "FIO", 59, 109));
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
    public void test_insertMany_replaceMany() throws ExecutionException, InterruptedException {
        truncateSpace(TEST_SPACE_NAME);
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        List<TarantoolTuple> tarantoolTuples = new ArrayList<>(3);
        tarantoolTuples.addAll(Arrays.asList(
            tupleFactory.create(Arrays.asList(123, null, "Jane Doe", 18, 999)),
            tupleFactory.create(Arrays.asList(456, null, "Jack the Ripper", 33, 111))
        ));

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insertMany(tarantoolTuples).get();
        assertEquals(insertTuples.size(), 2);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 123);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "Jane Doe");
        assertEquals(tuple.getInteger(3), 18);
        assertEquals(tuple.getInteger(4), 999);
        tuple = insertTuples.get(1);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 456);
        assertEquals(tuple.getString(2), "Jack the Ripper");
        assertEquals(tuple.getInteger(3), 33);
        assertEquals(tuple.getInteger(4), 111);

        tarantoolTuples.set(0, tupleFactory.create(Arrays.asList(123, null, "Jane Doe", 18, 990)));
        tarantoolTuples.add(tupleFactory.create(Arrays.asList(777, null, "Ostap Bender", 99, 777)));

        List<TarantoolTuple> duplicateTuples = Collections.unmodifiableList(tarantoolTuples);

        // Repeat the insert with the same IDs
        assertThrows(ExecutionException.class,
            () -> profileSpace.insertMany(duplicateTuples).get(),
            "Duplicate key exists in unique index 'primary' in space 'test_space'");

        // The data wasn't changed
        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(Conditions.any()).get();
        assertEquals(2, selectResult.size());
        assertEquals(selectResult.get(0).getInteger(4), 999);

        // Replace
        tarantoolTuples = Arrays.asList(
            tupleFactory.create(Arrays.asList(123, null, "John Doe", 21, 100)),
            tupleFactory.create(Arrays.asList(456, null, "Jack the Ripper", 44, 333)),
            tupleFactory.create(Arrays.asList(777, null, "Ostap Bender", 99, 777))
        );
        TarantoolResult<TarantoolTuple> replaceResult = profileSpace.replaceMany(tarantoolTuples).get();
        assertEquals(replaceResult.size(), 3);
        tuple = replaceResult.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 123);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "John Doe");
        assertEquals(tuple.getInteger(3), 21);
        assertEquals(tuple.getInteger(4), 100);
        tuple = replaceResult.get(1);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 456);
        assertEquals(tuple.getString(2), "Jack the Ripper");
        assertEquals(tuple.getInteger(3), 44);
        assertEquals(tuple.getInteger(4), 333);
        tuple = replaceResult.get(2);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 777);
        assertEquals(tuple.getString(2), "Ostap Bender");
        assertEquals(tuple.getInteger(3), 99);
        assertEquals(tuple.getInteger(4), 777);
    }

    @Test
    public void clusterUpdateTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(223, null, "Jane Doe", 10, 10);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(1, insertTuples.size());
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(5, tuple.size());
        assertEquals(223, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id

        Conditions conditions = Conditions.indexEquals(PK_FIELD_NAME, Collections.singletonList(223));

        TarantoolResult<TarantoolTuple> updateResult;

        updateResult = profileSpace.update(conditions, TupleOperations.add(3, 90).andAdd(4, 5)).get();
        assertEquals(223, updateResult.get(0).getInteger(0));
        assertEquals(100, updateResult.get(0).getInteger(3));
        assertEquals(15, updateResult.get(0).getInteger(4));
    }

    @Test
    public void test_clusterUpdate_shouldReturnTuple_ifSearchByFieldName() throws ExecutionException,
        InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space("cursor_test_space");

        int itemId = 223;
        List<Object> rowData = Arrays.asList(itemId, "Jane Doe", 10, null);
        TarantoolTuple tarantoolTuple = tupleFactory.create(rowData);

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.replace(tarantoolTuple).get();
        assertEquals(1, insertTuples.size());
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(4, tuple.size());
        assertEquals(itemId, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(3)); //bucket_id

        Conditions conditions = Conditions.equals("id", itemId);
        TarantoolResult<TarantoolTuple> updateResult = profileSpace
            .update(conditions, TupleOperations.set("year", 42)).get();
        assertEquals(itemId, updateResult.get(0).getInteger(0));
        assertEquals(42, updateResult.get(0).getInteger(2));
    }

    @Test
    public void test_crudMetadataResponse_shouldReturnTuple_withoutDDLMetadata()
        throws ExecutionException, InterruptedException {
        Integer id = 123000;
        String field1 = "Jane Doe";
        Integer field2 = 999;
        Boolean field3 = true;
        Double field4 = 123.456;

        List<Object> values = Arrays.asList(id, null, field1, field2);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);
        client.space("test_space").insert(tarantoolTuple).get();

        values = Arrays.asList(id, null, field3, field4);
        tarantoolTuple = tupleFactory.create(values);
        client.space("test_space_to_join").insert(tarantoolTuple).get();

        CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
            defaultResultMapper = mapperFactoryFactory.singleValueTupleResultMapperFactory()
            .withSingleValueRowsMetadataToTarantoolTupleResultMapper(defaultMapper, null);

        TarantoolResult<TarantoolTuple> result =
            client.callForSingleResult(
                "get_composite_data_with_crud",
                Collections.singletonList(id),
                defaultResultMapper
            ).get();
        assertEquals(1, result.size());
        TarantoolTuple tuple = result.get(0);
        assertEquals(id, tuple.getObject("id").orElse(null));
        assertEquals(field1, tuple.getObject("field1").orElse(null));
        assertEquals(field2, tuple.getObject("field2").orElse(null));
        assertEquals(field3, tuple.getObject("field3").orElse(null));
        assertEquals(field4, tuple.getObject("field4").orElse(null));
    }

    @Test
    public void test_universalConverter_shouldWorkWithBoxAndCrudCorrectly() {
        Integer id = 123000;
        String field1 = "Jane Doe";
        Integer field2 = 999;

        List<Object> values = Arrays.asList(id, null, field1, field2);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);
        client.space("test_space").insert(tarantoolTuple).join();

        MessagePackMapper valueMapper = client.getConfig().getMessagePackMapper();

        ResultMapperFactoryFactory factory =
            client.getResultMapperFactoryFactory();
        CallResultMapper callReturnMapper = factory.createMapper(valueMapper)
            .withSingleValueConverter(
                factory.createMapper(valueMapper)
                    .withArrayValueToTarantoolTupleResultConverter()
                    .withRowsMetadataToTarantoolTupleResultConverter()
                    .buildCallResultMapper()
            )
            .buildCallResultMapper();

        Object crudResult =
            client.call("crud.select", Collections.singletonList("test_space"), callReturnMapper).join();

        assertTrue(crudResult instanceof TarantoolTupleResultImpl);
        TarantoolTupleResultImpl tarantoolResult = (TarantoolTupleResultImpl) crudResult;
        assertEquals(1, tarantoolResult.size());
        TarantoolTuple tuple = tarantoolResult.get(0);
        assertEquals(id, tuple.getObject("id").orElse(null));
        assertEquals(field1, tuple.getObject("field1").orElse(null));
        assertEquals(field2, tuple.getObject("field2").orElse(null));

        Object boxResult =
            client.call("select_router_space", new ArrayList<>(), callReturnMapper).join();

        assertTrue(boxResult instanceof TarantoolTupleResultImpl);
        tarantoolResult = (TarantoolTupleResultImpl) boxResult;
        assertEquals(1, tarantoolResult.size());
        tuple = tarantoolResult.get(0);
        assertEquals(1, tuple.getObject(0).orElse(null));

        Object primitiveObject = client.call("returning_number", new ArrayList<>(), callReturnMapper).join();
        assertEquals(2, primitiveObject);
    }

    @Test
    public void clusterUpsertTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(301, null, "Jack Sparrow", 30, 0);
        TarantoolTuple tarantoolTuple = tupleFactory.create(values);

        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 301);

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

    @Test
    public void differentSpacesMappersTest() {
        List<Object> values = Arrays.asList(123000, null, "Jane Doe", 999);
        TarantoolTuple firstTuple = tupleFactory.create(values);

        values = Arrays.asList(123000, null, true, 123.456);
        TarantoolTuple secondTuple = tupleFactory.create(values);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<?>> futures = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            final int count = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    TarantoolResult<TarantoolTuple> newTuples;
                    for (int j = 0; j < 10; j++) {
                        if (count % 2 == 0) {
                            newTuples = client.space("test_space").replace(firstTuple).get();
                            assertTrue(newTuples.size() > 0);
                            assertEquals("Jane Doe", newTuples.get(0).getString("field1"));
                            assertEquals(999, newTuples.get(0).getInteger("field2"));
                        } else {
                            newTuples = client.space("test_space_to_join").replace(secondTuple).get();
                            assertTrue(newTuples.size() > 0);
                            assertEquals(true, newTuples.get(0).getBoolean("field3"));
                            assertEquals(123.456F, newTuples.get(0).getFloat("field4"));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, executor));
        }
        futures.forEach(CompletableFuture::join);
    }

    @Test
    public void singleResultWithConverterTest() throws Exception {
        MessagePackMapper mapper = client.getConfig().getMessagePackMapper();

        client.callForSingleResult(
                "get_array_as_single_result",
                Collections.singletonList(Arrays.asList(1, 2, 3)),
                v -> mapper.fromValue(v.asArrayValue(), (Class<List<Integer>>) (Class<?>) List.class)
            )
            .thenAccept(result -> assertEquals(3, result.size()))
            .get();
    }

    @Test
    public void multiResultWithConverterTest() {
        TarantoolSpaceMetadata metadata = client.space("second_test_space").getMetadata();
        MessagePackMapper mapper = client.getConfig().getMessagePackMapper();

        List<TarantoolTuple> tuples = new ArrayList<>(100);
        for (int i = 100; i <= 200; i++) {
            tuples.add(tupleFactory.create(Arrays.asList(123000 + i, null, "Jane Doe " + i, 999 + i)));
        }
        CompletableFuture.allOf(
            tuples.stream()
                .map(t -> client.space("second_test_space").insert(t))
                .toArray(CompletableFuture[]::new)
        ).thenCompose(__ ->
            client.<TarantoolTuple, ArrayList<TarantoolTuple>>callForMultiResult(
                "get_rows_as_multi_result",
                Collections.singletonList("second_test_space"),
                ArrayList::new,
                v -> new TarantoolTupleImpl(v.asArrayValue(), mapper, metadata)
            )
        ).thenAccept(result -> assertEquals(101, result.size()));
    }

    @Test
    public void singleResultWithClassTest() throws ExecutionException, InterruptedException {
        List<Integer> result = client.callForSingleResult(
            "get_array_as_single_result",
            Collections.singletonList(Arrays.asList(1, 2, 3)),
            (Class<List<Integer>>) (Class<?>) List.class
        ).get();
        assertEquals(Arrays.asList(1, 2, 3), result);
    }

    @Test
    public void singleResultWithErrorTest() throws ExecutionException, InterruptedException {
        Exception thrown = null;
        try {
            client.callForSingleResult(
                "returning_error",
                Collections.singletonList("Some error"),
                String.class
            ).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof TarantoolInternalException) &&
                !e.getCause().getMessage().contains("Some error")) {
                throw e;
            }
            thrown = e;
        }
        assertNotNull(thrown, "Exception was not thrown");
    }

    @Test
    public void multiResultWithClassTest() throws ExecutionException, InterruptedException {
        List<Integer> result = client.callForMultiResult(
            "get_array_as_multi_result",
            Collections.singletonList(Arrays.asList(3, 2, 1)),
            ArrayList::new,
            Integer.class
        ).get();
        assertEquals(Arrays.asList(3, 2, 1), result);
    }

    @Test
    public void multiResultWithErrorTest() throws ExecutionException, InterruptedException {
        Exception thrown = null;
        try {
            client.callForMultiResult(
                "returning_error",
                Collections.singletonList("Some error"),
                ArrayList::new,
                String.class
            ).get();
        } catch (Exception e) {
            if (!(e.getCause() instanceof TarantoolInternalException) &&
                !e.getCause().getMessage().contains("Some error")) {
                throw e;
            }
            thrown = e;
        }
        assertNotNull(thrown, "Exception was not thrown");
    }
}
