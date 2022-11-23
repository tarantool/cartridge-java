package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.proxy.ProxyInsertManyOptions;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Alexey Kuzin
 */
public class ProxySpaceInsertManyOptionsIT extends SharedCartridgeContainer {

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

    public static String USER_NAME;
    public static String PASSWORD;

    private static final String TEST_SPACE_NAME = "test__profile";

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
                .withRollbackOnError(false)
                .withStopOnError(false)
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
        assertEquals(requestConfigTimeout, ((HashMap) crudInsertManyOpts.get(0)).get("timeout"));

        // with option timeout
        tarantoolTuples = Arrays.asList(
            tupleFactory.create(3, null, "FIO", 50, 100),
            tupleFactory.create(4, null, "KEK", 75, 125)
        );

        profileSpace.insertMany(
            tarantoolTuples,
            ProxyInsertManyOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudInsertManyOpts = client.eval("return crud_insert_many_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudInsertManyOpts.get(0)).get("timeout"));
    }
}
