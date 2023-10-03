package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.InsertOptions;
import io.tarantool.driver.api.space.options.SelectOptions;
import io.tarantool.driver.api.space.options.ProxyInsertOptions;
import io.tarantool.driver.api.space.options.ProxySelectOptions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.integration.Utils;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Artyom Dubinin
 */
public class ProxySpaceInsertOptionsIT extends SharedCartridgeContainer {

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

    public static String USER_NAME;
    public static String PASSWORD;

    private static final String TEST_SPACE_NAME = "test__profile";
    private static final String PK_FIELD_NAME = "profile_id";

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
        initClient();
    }

    private static void initClient() {
        TarantoolClientConfig config = TarantoolClientConfig.builder()
            .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
            .withConnectTimeout(1000)
            .withReadTimeout(1000)
            .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
            config, container.getRouterHost(), container.getRouterPort());
        client = new ProxyTarantoolTupleClient(clusterClient);
    }

    @BeforeEach
    public void truncateSpace() {
        truncateSpace(TEST_SPACE_NAME);
    }

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
    }

    @Test
    public void withTimeout() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        int requestConfigTimeout = client.getConfig().getRequestTimeout();
        int customRequestTimeout = requestConfigTimeout * 2;

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);

        // with config timeout
        profileSpace.insert(tarantoolTuple).get();
        List<?> crudInsertOpts = client.eval("return crud_insert_opts").get();
        assertNull(((HashMap) crudInsertOpts.get(0)).get("timeout"));

        profileSpace.delete(Conditions.equals(PK_FIELD_NAME, 1)).get();
        profileSpace.insert(tarantoolTuple, ProxyInsertOptions.create()).get();
        crudInsertOpts = client.eval("return crud_insert_opts").get();
        assertNull(((HashMap) crudInsertOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.delete(Conditions.equals(PK_FIELD_NAME, 1)).get();
        profileSpace.insert(
            tarantoolTuple,
            ProxyInsertOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudInsertOpts = client.eval("return crud_insert_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudInsertOpts.get(0)).get("timeout"));
    }

    @Test
    public void withBucketIdTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions condition = Conditions.equals(PK_FIELD_NAME, 1);

        // without bucket id
        TarantoolResult<TarantoolTuple> insertResult = profileSpace.insert(tarantoolTuple).get();
        assertEquals(1, insertResult.size());

        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(condition).get();
        assertEquals(1, selectResult.size());
        profileSpace.delete(condition).get();

        // with bucket id
        Integer otherStorageBucketId = client.callForSingleResult(
            "get_other_storage_bucket_id",
            Collections.singletonList(tarantoolTuple.getInteger(0)),
            Integer.class).get();
        InsertOptions insertOptions = ProxyInsertOptions.create().withBucketId(otherStorageBucketId);
        insertResult = profileSpace.insert(tarantoolTuple, insertOptions).get();
        assertEquals(1, insertResult.size());

        selectResult = profileSpace.select(condition).get();
        assertEquals(0, selectResult.size());

        ProxySelectOptions selectOptions = ProxySelectOptions.create().withBucketId(otherStorageBucketId);
        selectResult = profileSpace.select(condition, selectOptions).get();
        assertEquals(1, selectResult.size());
    }

    @Test
    public void withBucketIdFromClientTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions condition = Conditions.equals(PK_FIELD_NAME, 1);

        Integer bucketId = Utils.getBucketIdStrCRC32(client,
            Collections.singletonList(tarantoolTuple.getInteger(0)));
        InsertOptions insertOptions = ProxyInsertOptions.create().withBucketId(bucketId);

        TarantoolResult<TarantoolTuple> insertResult = profileSpace.insert(tarantoolTuple, insertOptions).get();
        assertEquals(1, insertResult.size());

        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(condition).get();
        assertEquals(1, selectResult.size());

        SelectOptions selectOptions = ProxySelectOptions.create().withBucketId(bucketId);
        selectResult = profileSpace.select(condition, selectOptions).get();
        assertEquals(1, selectResult.size());
    }

    private Integer getBucketIdFromTarantool(List<Object> key) throws ExecutionException, InterruptedException {
        return client.callForSingleResult(
            "vshard.router.bucket_id_strcrc32",
            Collections.singletonList(key),
            Integer.class
        ).get();
    }

    @Test
    public void withBucketIdClientComputationTest() throws ExecutionException, InterruptedException {
        List<List<Object>> keys = Arrays.asList(
            Collections.singletonList(1),
            Arrays.asList(1, "FIO"),
            Arrays.asList(1, true, "FIO", 'm', 100.123)
        );

        for (List<Object> key : keys) {
            assertEquals(Utils.getBucketIdStrCRC32(client, key), getBucketIdFromTarantool(key));
        }
    }

    @Test
    public void withBucketIdMoreThanLimitTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);

        Integer bucketsCount = client.callForSingleResult("vshard.router.bucket_count", Integer.class).get();
        InsertOptions insertOptions = ProxyInsertOptions.create().withBucketId(bucketsCount * 2);

        ExecutionException e = assertThrows(ExecutionException.class, () -> {
            profileSpace.insert(tarantoolTuple, insertOptions).get();
        });
        assertTrue(e.getCause() instanceof TarantoolInternalException);
        assertTrue(e.getCause().getMessage().contains("Bucket is unreachable: bucket id is out of range"));
    }

    @Test
    public void withBucketIdNull() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions condition = Conditions.equals(PK_FIELD_NAME, 1);

        InsertOptions insertOptions = ProxyInsertOptions.create().withBucketId(null);

        TarantoolResult<TarantoolTuple> insertResult = profileSpace.insert(tarantoolTuple, insertOptions).get();
        assertEquals(1, insertResult.size());

        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(condition).get();
        assertEquals(1, selectResult.size());
    }

    @Test
    public void withBucketIdWithNegativeValues() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);

        InsertOptions insertOptions = ProxyInsertOptions.create().withBucketId(-1);

        ExecutionException e = assertThrows(ExecutionException.class, () -> {
            profileSpace.insert(tarantoolTuple, insertOptions).get();
        });
        assertTrue(e.getCause() instanceof TarantoolInternalException);
        assertTrue(e.getCause().getMessage().contains("Bucket is unreachable: bucket id is out of range"));
    }

    @Test
    public void withFieldsTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);

        // without fields
        TarantoolResult<TarantoolTuple> insertResult = profileSpace.insert(tarantoolTuple).get();
        assertEquals(1, insertResult.size());

        TarantoolTuple tuple = insertResult.get(0);
        assertEquals(5, tuple.size());
        assertEquals(1, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals("FIO", tuple.getString(2));
        assertEquals(50, tuple.getInteger(3));
        assertEquals(100, tuple.getInteger(4));

        // with fields
        profileSpace.delete(Conditions.equals(PK_FIELD_NAME, 1)).get();
        InsertOptions options = ProxyInsertOptions.create().withFields(Arrays.asList("profile_id", "fio"));
        insertResult = profileSpace.insert(tarantoolTuple, options).get();
        assertEquals(1, insertResult.size());

        tuple = insertResult.get(0);
        assertEquals(2, tuple.size());
        assertEquals(1, tuple.getInteger(0));
        assertEquals("FIO", tuple.getString(1));
    }
}
