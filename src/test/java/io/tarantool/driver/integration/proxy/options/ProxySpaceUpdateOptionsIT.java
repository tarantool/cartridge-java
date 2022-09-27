package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.space.options.proxy.ProxyUpdateOptions;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Artyom Dubinin
 */
public class ProxySpaceUpdateOptionsIT extends SharedCartridgeContainer {

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
        profileSpace.update(conditions, tarantoolTuple).get();
        List<?> crudUpdateOpts = client.eval("return crud_update_opts").get();
        assertEquals(requestConfigTimeout, ((HashMap) crudUpdateOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.update(
                conditions,
                tarantoolTuple,
                ProxyUpdateOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudUpdateOpts = client.eval("return crud_update_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudUpdateOpts.get(0)).get("timeout"));
    }

    @Test
    public void withBucketIdTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);

        // without bucket id
        profileSpace.update(conditions, tarantoolTuple).get();
        List<?> crudUpdateOpts = client.eval("return crud_update_opts").get();
        assertNull(((HashMap) crudUpdateOpts.get(0)).get("bucket_id"));

        // with bucket id
        Integer bucketId = 1;
        profileSpace.update(
                conditions,
                tarantoolTuple,
                ProxyUpdateOptions.create().withBucketId(bucketId)
        ).get();
        crudUpdateOpts = client.eval("return crud_update_opts").get();
        assertEquals(bucketId, ((HashMap) crudUpdateOpts.get(0)).get("bucket_id"));
    }
}
