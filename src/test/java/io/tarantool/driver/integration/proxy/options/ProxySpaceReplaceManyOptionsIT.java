package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.options.interfaces.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.interfaces.ReplaceManyOptions;
import io.tarantool.driver.api.space.options.enums.RollbackOnError;
import io.tarantool.driver.api.space.options.enums.StopOnError;
import io.tarantool.driver.api.space.options.proxy.ProxyReplaceManyOptions;
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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Alexey Kuzin
 */
public class ProxySpaceReplaceManyOptionsIT extends SharedCartridgeContainer {

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
        profileSpace.replaceMany(tarantoolTuples).get();
        List<?> crudReplaceManyOpts = client.eval("return crud_replace_many_opts").get();
        assertEquals(true, ((HashMap) crudReplaceManyOpts.get(0)).get("rollback_on_error"));
        assertEquals(true, ((HashMap) crudReplaceManyOpts.get(0)).get("stop_on_error"));

        // with custom values
        profileSpace.replaceMany(
            tarantoolTuples,
            ProxyReplaceManyOptions.create()
                .withRollbackOnError(RollbackOnError.FALSE)
                .withStopOnError(StopOnError.FALSE)
        ).get();
        crudReplaceManyOpts = client.eval("return crud_replace_many_opts").get();
        assertEquals(false, ((HashMap) crudReplaceManyOpts.get(0)).get("rollback_on_error"));
        assertEquals(false, ((HashMap) crudReplaceManyOpts.get(0)).get("stop_on_error"));
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
        profileSpace.replaceMany(tarantoolTuples).get();
        List<?> crudReplaceManyOpts = client.eval("return crud_replace_many_opts").get();
        assertNull(((HashMap) crudReplaceManyOpts.get(0)).get("timeout"));

        profileSpace.replaceMany(tarantoolTuples, ProxyReplaceManyOptions.create()).get();
        crudReplaceManyOpts = client.eval("return crud_replace_many_opts").get();
        assertNull(((HashMap) crudReplaceManyOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.replaceMany(
            tarantoolTuples,
            ProxyReplaceManyOptions.create().withTimeout(customRequestTimeout)
        ).get();
        crudReplaceManyOpts = client.eval("return crud_replace_many_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudReplaceManyOpts.get(0)).get("timeout"));
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
        TarantoolResult<TarantoolTuple> replaceResult = profileSpace.replaceMany(tarantoolTuples).get();
        replaceResult.sort(Comparator.comparing(tuple -> tuple.getInteger(0)));
        assertEquals(2, replaceResult.size());

        for (int i = 0; i < replaceResult.size(); i++) {
            TarantoolTuple tuple = replaceResult.get(i);
            assertEquals(5, tuple.size());
            assertEquals(i, tuple.getInteger(0));
            assertNotNull(tuple.getInteger(1)); //bucket_id
            assertEquals(String.valueOf(i), tuple.getString(2));
            assertEquals(i, tuple.getInteger(3));
            assertEquals(i, tuple.getInteger(4));
        }

        // with fields
        ReplaceManyOptions options = ProxyReplaceManyOptions.create().withFields(Arrays.asList("profile_id", "fio"));
        replaceResult = profileSpace.replaceMany(tarantoolTuples, options).get();
        replaceResult.sort(Comparator.comparing(tuple -> tuple.getInteger(0)));
        assertEquals(2, replaceResult.size());

        for (int i = 0; i < replaceResult.size(); i++) {
            TarantoolTuple tuple = replaceResult.get(i);
            assertEquals(2, tuple.size());
            assertEquals(i, tuple.getInteger(0));
            assertEquals(String.valueOf(i), tuple.getString(1));
        }
    }
}
