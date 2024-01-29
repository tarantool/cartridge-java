package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.InsertManyOptions;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;
import io.tarantool.driver.api.space.options.crud.enums.RollbackOnError;
import io.tarantool.driver.api.space.options.crud.enums.StopOnError;
import io.tarantool.driver.api.space.options.ProxyInsertManyOptions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class ProxySpaceInsertManyOptionsIT extends SharedCartridgeContainer {

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
    private static final String TEST_SPACE_NAME = "test__profile";
    private static final String PK_FIELD_NAME = "profile_id";
    public static String USER_NAME;
    public static String PASSWORD;
    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

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

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
    }

    @BeforeEach
    public void truncateSpace() {
        truncateSpace(TEST_SPACE_NAME);
    }

    @Test
    public void withStopOnError_withRollbackOnError() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        List<TarantoolTuple> tarantoolTuples = Arrays.asList(
            tupleFactory.create(1, null, "FIO", 50, 100),
            tupleFactory.create(2, null, "KEK", 75, 125)
        );

        // with default values
        profileSpace.insertMany(tarantoolTuples).get();
        List<?> crudInsertManyOpts = client.eval("return crud_insert_many_opts").get();
        assertEquals(true, ((HashMap) crudInsertManyOpts.get(0)).get("rollback_on_error"));
        assertEquals(true, ((HashMap) crudInsertManyOpts.get(0)).get("stop_on_error"));

        // with custom values
        tarantoolTuples = Arrays.asList(
            tupleFactory.create(3, null, "FIO", 50, 100),
            tupleFactory.create(4, null, "KEK", 75, 125)
        );

        profileSpace.insertMany(
            tarantoolTuples,
            ProxyInsertManyOptions.create()
                .withRollbackOnError(RollbackOnError.FALSE)
                .withStopOnError(StopOnError.FALSE)
        ).get();
        crudInsertManyOpts = client.eval("return crud_insert_many_opts").get();
        assertEquals(false, ((HashMap) crudInsertManyOpts.get(0)).get("rollback_on_error"));
        assertEquals(false, ((HashMap) crudInsertManyOpts.get(0)).get("stop_on_error"));
    }

    @Test
    public void withTimeout() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        int requestConfigTimeout = client.getConfig().getRequestTimeout();
        int customRequestTimeout = requestConfigTimeout * 2;

        List<TarantoolTuple> tarantoolTuples = Arrays.asList(
            tupleFactory.create(1, null, "FIO", 50, 100),
            tupleFactory.create(2, null, "KEK", 75, 125)
        );

        // with config timeout
        profileSpace.insertMany(tarantoolTuples).get();
        List<?> crudInsertManyOpts = client.eval("return crud_insert_many_opts").get();
        assertNull(((HashMap) crudInsertManyOpts.get(0)).get("timeout"));

        tarantoolTuples = Arrays.asList(
            tupleFactory.create(3, null, "FIO", 50, 100),
            tupleFactory.create(4, null, "KEK", 75, 125)
        );
        profileSpace.insertMany(tarantoolTuples, ProxyInsertManyOptions.create()).get();
        crudInsertManyOpts = client.eval("return crud_insert_many_opts").get();
        assertNull(((HashMap) crudInsertManyOpts.get(0)).get("timeout"));

        // with option timeout
        tarantoolTuples = Arrays.asList(
            tupleFactory.create(5, null, "FIO", 50, 100),
            tupleFactory.create(6, null, "KEK", 75, 125)
        );

        profileSpace.insertMany(
            tarantoolTuples,
            ProxyInsertManyOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudInsertManyOpts = client.eval("return crud_insert_many_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudInsertManyOpts.get(0)).get("timeout"));
    }

    @Test
    public void withFieldsTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        List<TarantoolTuple> tarantoolTuples = Arrays.asList(
            tupleFactory.create(0, null, "0", 0, 0),
            tupleFactory.create(1, null, "1", 1, 1)
        );

        // without fields
        TarantoolResult<TarantoolTuple> insertResult = profileSpace.insertMany(tarantoolTuples).get();
        assertEquals(2, insertResult.size());
        insertResult.sort(Comparator.comparing(tuple -> tuple.getInteger(0)));

        for (int i = 0; i < insertResult.size(); i++) {
            TarantoolTuple tuple = insertResult.get(i);
            assertEquals(5, tuple.size());
            assertEquals(i, tuple.getInteger(0));
            assertNotNull(tuple.getInteger(1)); //bucket_id
            assertEquals(String.valueOf(i), tuple.getString(2));
            assertEquals(i, tuple.getInteger(3));
            assertEquals(i, tuple.getInteger(4));
        }

        // with fields
        profileSpace.delete(Conditions.equals(PK_FIELD_NAME, 0)).get();
        profileSpace.delete(Conditions.equals(PK_FIELD_NAME, 1)).get();
        InsertManyOptions options = ProxyInsertManyOptions.create().withFields(Arrays.asList("profile_id", "fio"));
        insertResult = profileSpace.insertMany(tarantoolTuples, options).get();
        assertEquals(2, insertResult.size());
        insertResult.sort(Comparator.comparing(tuple -> tuple.getInteger(0)));

        for (int i = 0; i < insertResult.size(); i++) {
            TarantoolTuple tuple = insertResult.get(i);
            assertEquals(2, tuple.size());
            assertEquals(i, tuple.getInteger(0));
            assertEquals(String.valueOf(i), tuple.getString(1));
        }
    }

    @Test
    public void withVshardRouterTest() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        List<TarantoolTuple> tarantoolTuples = Arrays.asList(
            tupleFactory.create(0, null, "0", 0, 0),
            tupleFactory.create(1, null, "1", 1, 1));

        final String groupName = "default";
        InsertManyOptions<ProxyInsertManyOptions> options = ProxyInsertManyOptions.create()
                                                                                  .withVshardRouter(groupName);

        TarantoolResult<TarantoolTuple> insertManyResult = profileSpace.insertMany(tarantoolTuples, options).join();
        List<?> crudInsertManyOpts = client.eval("return crud_insert_many_opts").join();
        assertEquals(2, insertManyResult.size());
        assertEquals(groupName, ((HashMap<?, ?>) crudInsertManyOpts.get(0)).get(ProxyOption.VSHARD_ROUTER.toString()));
    }

    @Test
    public void withFetchLatestMetadata() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        List<TarantoolTuple> tarantoolTuples = Arrays.asList(
            tupleFactory.create(0, null, "0", 0, 0),
            tupleFactory.create(1, null, "1", 1, 1)
                                                            );
        InsertManyOptions<ProxyInsertManyOptions> options = ProxyInsertManyOptions.create().fetchLatestMetadata();

        assertTrue(options.getFetchLatestMetadata().isPresent());
        assertTrue(options.getFetchLatestMetadata().get());

        TarantoolResult<TarantoolTuple> insertManyResult = profileSpace.insertMany(tarantoolTuples, options).join();
        List<?> crudInsertManyOpts = client.eval("return crud_insert_many_opts").join();

        assertEquals(2, insertManyResult.size());
        Assertions.assertEquals(true, ((HashMap<?, ?>) crudInsertManyOpts.get(0))
            .get(ProxyOption.FETCH_LATEST_METADATA.toString()));
    }
}
