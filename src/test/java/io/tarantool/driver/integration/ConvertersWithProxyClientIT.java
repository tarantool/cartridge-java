package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.UUID;

/**
 * @author Artyom Dubinin
 */
public class ConvertersWithProxyClientIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
            new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
    public static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        client = setupClient();
    }

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
        return TarantoolClientFactory.createClient()
                .withAddress(container.getHost(), container.getPort())
                .withCredentials(container.getUsername(), container.getPassword())
                .withProxyMethodMapping()
                .build();
    }

    @Test
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithUUID")
    public void test_crudSelect_shouldReturnTupleWithUUID() throws Exception {
        //given
        UUID uuid = UUID.randomUUID();
        client.space("space_with_uuid")
                .insert(tupleFactory.create(1, uuid)).get();

        //when
        TarantoolTuple fields = client
                .space("space_with_uuid")
                .select(Conditions.equals("id", 1)).get().get(0);

        //then
        Assertions.assertEquals(uuid, fields.getUUID("uuid_field"));
    }
}
