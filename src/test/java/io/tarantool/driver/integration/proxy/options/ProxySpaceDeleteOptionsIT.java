package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.InsertOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyDeleteOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyInsertOptions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Artyom Dubinin
 */
public class ProxySpaceDeleteOptionsIT extends SharedCartridgeContainer {

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

        // with config timeout
        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);
        profileSpace.delete(conditions).get();
        List<?> crudDeleteOpts = client.eval("return crud_delete_opts").get();
        assertEquals(requestConfigTimeout, ((HashMap) crudDeleteOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.delete(
            conditions,
            ProxyDeleteOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudDeleteOpts = client.eval("return crud_delete_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudDeleteOpts.get(0)).get("timeout"));
    }

    @Test
    public void withBucketIdTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions condition = Conditions.equals(PK_FIELD_NAME, 1);

        // with bucket id
        Integer otherStorageBucketId = client.callForSingleResult(
            "get_other_storage_bucket_id",
            Collections.singletonList(tarantoolTuple.getInteger(0)),
            Integer.class).get();
        InsertOptions insertOptions = ProxyInsertOptions.create().withBucketId(otherStorageBucketId);
        TarantoolResult<TarantoolTuple> insertResult = profileSpace.insert(tarantoolTuple, insertOptions).get();
        assertEquals(1, insertResult.size());

        TarantoolResult<TarantoolTuple> selectResult = profileSpace.delete(condition).get();
        assertEquals(0, selectResult.size());

        ProxyDeleteOptions deleteOptions = ProxyDeleteOptions.create().withBucketId(otherStorageBucketId);
        selectResult = profileSpace.delete(condition, deleteOptions).get();
        assertEquals(1, selectResult.size());
    }
}
