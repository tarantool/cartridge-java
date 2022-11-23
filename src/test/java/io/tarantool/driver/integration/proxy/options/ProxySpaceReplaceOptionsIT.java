package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.ReplaceOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyDeleteOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyReplaceOptions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
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
public class ProxySpaceReplaceOptionsIT extends SharedCartridgeContainer {

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
        profileSpace.replace(tarantoolTuple).get();
        List<?> crudReplaceOpts = client.eval("return crud_replace_opts").get();
        assertEquals(requestConfigTimeout, ((HashMap) crudReplaceOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.replace(
            tarantoolTuple,
            ProxyReplaceOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudReplaceOpts = client.eval("return crud_replace_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudReplaceOpts.get(0)).get("timeout"));
    }

    @Test
    public void withBucketIdTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions condition = Conditions.equals(PK_FIELD_NAME, 1);

        // without bucket id
        TarantoolResult<TarantoolTuple> replaceResult = profileSpace.replace(tarantoolTuple).get();
        assertEquals(1, replaceResult.size());

        TarantoolResult<TarantoolTuple> deleteResult = profileSpace.delete(condition).get();
        assertEquals(1, deleteResult.size());

        // with bucket id
        Integer otherStorageBucketId = client.callForSingleResult(
            "get_other_storage_bucket_id",
            Collections.singletonList(tarantoolTuple.getInteger(0)),
            Integer.class).get();
        ReplaceOptions replaceOptions = ProxyReplaceOptions.create().withBucketId(otherStorageBucketId);
        replaceResult = profileSpace.replace(tarantoolTuple, replaceOptions).get();
        assertEquals(1, replaceResult.size());

        deleteResult = profileSpace.delete(condition).get();
        assertEquals(0, deleteResult.size());

        ProxyDeleteOptions deleteOptions = ProxyDeleteOptions.create().withBucketId(otherStorageBucketId);
        deleteResult = profileSpace.delete(condition, deleteOptions).get();
        assertEquals(1, deleteResult.size());
    }
}
