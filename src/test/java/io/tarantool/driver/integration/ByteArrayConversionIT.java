package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author Alexey Kuzin
 */
public class ByteArrayConversionIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
            new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
    private static final String SPACE = "test_space_with_byte_array";
    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
        client = setupClient();
    }

    private static ProxyTarantoolTupleClient setupClient() {
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000)
                .withReadTimeout(1000)
                .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
                config, container.getHost(), container.getPort());
        return new ProxyTarantoolTupleClient(clusterClient);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void test_crudSelect_shouldReturnTupleWithByteArray() throws Exception {
        //given
        client.space(SPACE)
                .insert(tupleFactory.create(1, null, new byte[]{1, 2, 3, 4})).get();

        //when
        TarantoolTuple fields = client
                .space(SPACE)
                .select(Conditions.equals("id", 1)).get().get(0);

        //then
        Assertions.assertNotNull(fields);
        Assertions.assertArrayEquals(new byte[]{1, 2, 3, 4}, fields.getByteArray("value"));
    }
}
