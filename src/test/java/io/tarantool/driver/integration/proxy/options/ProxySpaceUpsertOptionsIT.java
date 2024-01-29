package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.ProxyUpsertOptions;
import io.tarantool.driver.api.space.options.UpsertOptions;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Artyom Dubinin
 */
public class ProxySpaceUpsertOptionsIT extends SharedCartridgeContainer {

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
        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);

        // with config timeout
        profileSpace.upsert(conditions, tarantoolTuple, TupleOperations.set("age", 50)).get();
        List<?> crudUpsertOpts = client.eval("return crud_upsert_opts").get();
        assertNull(((HashMap) crudUpsertOpts.get(0)).get("timeout"));

        profileSpace.upsert(conditions, tarantoolTuple, TupleOperations.set("age", 50),
            ProxyUpsertOptions.create()).get();
        crudUpsertOpts = client.eval("return crud_upsert_opts").get();
        assertNull(((HashMap) crudUpsertOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.upsert(
            conditions,
            tarantoolTuple,
            TupleOperations.set("age", 50),
            ProxyUpsertOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudUpsertOpts = client.eval("return crud_upsert_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudUpsertOpts.get(0)).get("timeout"));
    }

    @Test
    public void withBucketIdTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);

        // without bucket id
        profileSpace.upsert(conditions, tarantoolTuple, TupleOperations.set("age", 50)).get();
        List<?> crudUpsertOpts = client.eval("return crud_upsert_opts").get();
        assertNull(((HashMap) crudUpsertOpts.get(0)).get("bucket_id"));

        // with bucket id
        Integer bucketId = 1;
        profileSpace.upsert(
            conditions,
            tarantoolTuple,
            TupleOperations.set("age", 50),
            ProxyUpsertOptions.create().withBucketId(bucketId)
        ).get();
        crudUpsertOpts = client.eval("return crud_upsert_opts").get();
        assertEquals(bucketId, ((HashMap) crudUpsertOpts.get(0)).get("bucket_id"));
    }

    @Test
    public void withVshardRouterTest() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);

        final String groupName = "default";
        UpsertOptions<ProxyUpsertOptions> options = ProxyUpsertOptions.create().withVshardRouter(groupName);

        profileSpace.upsert(conditions, tarantoolTuple, TupleOperations.set("age", 50), options).join();
        List<?> crudUpsertOpts = client.eval("return crud_upsert_opts").join();

        assertEquals(groupName, ((HashMap<?, ?>) crudUpsertOpts.get(0)).get(ProxyOption.VSHARD_ROUTER.toString()));
    }

    @Test
    public void withFetchLatestMetadata() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);
        UpsertOptions<ProxyUpsertOptions> options = ProxyUpsertOptions.create().fetchLatestMetadata();

        assertTrue(options.getFetchLatestMetadata().isPresent());
        assertTrue(options.getFetchLatestMetadata().get());

        profileSpace.upsert(conditions, tarantoolTuple, TupleOperations.set("age", 50), options).join();
        List<?> crudUpsertOpts = client.eval("return crud_upsert_opts").join();

        Assertions.assertEquals(true, ((HashMap<?, ?>) crudUpsertOpts.get(0))
            .get(ProxyOption.FETCH_LATEST_METADATA.toString()));
    }
}
