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
import io.tarantool.driver.api.space.options.proxy.ProxySelectOptions;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Artyom Dubinin
 */
public class ProxySpaceSelectOptionsIT extends SharedCartridgeContainer {

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
    public void withBatchSizeTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple;

        for (int i = 0; i < 100; i++) {
            tarantoolTuple = tupleFactory.create(i, null, "FIO", i, i);
            profileSpace.insert(tarantoolTuple).get();
        }

        Conditions conditions = Conditions.greaterOrEquals(PK_FIELD_NAME, 0).withLimit(10);

        // without batchSize
        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(conditions).get();
        assertEquals(10, selectResult.size());
        List<?> crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(null, ((HashMap) crudSelectOpts.get(0)).get("batch_size"));

        // with batchSize
        selectResult = profileSpace.select(
            conditions,
            ProxySelectOptions.create().withBatchSize(5)
        ).get();
        assertEquals(10, selectResult.size());
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(5, ((HashMap) crudSelectOpts.get(0)).get("batch_size"));
    }

    @Test
    public void withTimeout() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
                client.space(TEST_SPACE_NAME);

        int requestConfigTimeout = client.getConfig().getRequestTimeout();
        int customRequestTimeout = requestConfigTimeout * 2;

        // with config timeout
        profileSpace.select(Conditions.any()).get();
        List<?> crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(requestConfigTimeout, ((HashMap) crudSelectOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.select(
                Conditions.any(),
                ProxySelectOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudSelectOpts.get(0)).get("timeout"));
    }
}
