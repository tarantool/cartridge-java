package io.tarantool.driver.integration;


import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.converters.Interval;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Artyom Dubinin
 */
public class ConvertersWithClusterClientIT extends SharedTarantoolContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
    public static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    @BeforeAll
    public static void setUp() throws Exception {
        startContainer();
        client = setupClient();
    }

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
        return TarantoolClientFactory.createClient()
            .withAddress(container.getHost(), container.getPort())
            .withCredentials(container.getUsername(), container.getPassword())
            .build();
    }

    @Test
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithUUID")
    public void test_boxSelect_shouldReturnTupleWithUUID() throws Exception {
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

    @Test
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithInstant")
    public void test_boxSelect_shouldReturnTupleWithInterval() throws Exception {
        //given
        Interval interval = new Interval().setDay(1).setSec(123);
        client.space("space_with_interval")
            .insert(tupleFactory.create(1, interval)).get();

        //when
        TarantoolTuple fields = client
            .space("space_with_interval")
            .select(Conditions.equals("id", 1)).get().get(0);

        //then
        Assertions.assertEquals(interval, fields.getInterval("interval_field"));
    }

    @Test
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithInstant")
    public void test_boxSelect_shouldReturnTupleWithInstant() throws Exception {
        //given
        Instant instant = Instant.now();
        client.space("space_with_instant")
                .insert(tupleFactory.create(1, instant)).get();

        //when
        TarantoolTuple fields = client
                .space("space_with_instant")
                .select(Conditions.equals("id", 1)).get().get(0);

        //then
        Assertions.assertEquals(instant, fields.getInstant("instant_field"));
    }

    @Test
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithInstant")
    public void test_eval_shouldReturnDatetimeWithoutNsec() throws Exception {
        Instant expected = LocalDateTime.of(1, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);
        List<?> result = client.eval("return require('datetime').new({year = 1})").get();

        Assertions.assertEquals(expected, result.get(0));
    }

    @Test
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithVarbinary")
    public void test_boxOperations_shouldWorkWithVarbinary() throws Exception {
        //given
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
        List<Byte> byteList = Utils.convertBytesToByteList(bytes);
        client.space("space_with_varbinary")
            .insert(tupleFactory.create(1, bytes)).get();

        //when
        TarantoolTuple fields = client
            .space("space_with_varbinary")
            .select(Conditions.equals("id", 1)).get().get(0);

        //then
        byte[] bytesFromTarantool = fields.getByteArray("varbinary_field");
        List<Byte> byteListFromTarantool = Utils.convertBytesToByteList(bytesFromTarantool);
        Assertions.assertEquals(byteList, byteListFromTarantool);
    }
}
