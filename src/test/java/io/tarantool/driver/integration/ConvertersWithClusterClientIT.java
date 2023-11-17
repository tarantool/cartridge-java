package io.tarantool.driver.integration;


import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
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

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
        "Construct 'compact' value (zero nanoseconds, offset and timezone)" +
        " | new({year = 2023, month = 10, day = 25, hour = 13, min = 55, sec = 17})" +
        " | 2023-10-25T13:55:17Z",
        "Construct 'complete' (has nanos) value" +
        " | new({year = 2023, month = 10, day = 25, hour = 13, min = 55, sec = 17, usec = 71983})" +
        " | 2023-10-25T13:55:17.071983Z",
        "Construct 'complete' (has nanos and positive offset) value" +
        " | new({year = 2023, month = 10, day = 25, hour = 13, min = 55, sec = 17, usec = 71983, tzoffset = 0+180})" +
        " | 2023-10-25T13:55:17.071983+03:00",
        "Construct 'complete' (has nanos and negative offset) value" +
        " | new({year = 2023, month = 10, day = 25, hour = 13, min = 55, sec = 17, usec = 71983, tzoffset = 0-180})" +
        " | 2023-10-25T13:55:17.071983-03:00",
        "Construct 'complete' (has nanos and timezone) value" +
        " | new({year = 2023, month = 10, day = 25, hour = 13, min = 55, sec = 17, usec = 71983, tz = " +
        "'Europe/Isle_of_Man'})" +
        " | 2023-10-25T13:55:17.071983+01:00",
        "Parse with default format" +
        " | parse('1970-01-01T00:00:00Z')" +
        " | 1970-01-01T00:00:00Z",
        "Parse with ISO8601 format and offset" +
        " | parse('1970-01-01T00:00:00', {format = 'iso8601', tzoffset = 180})" +
        " | 1970-01-01T00:00:00+03:00",
        "Parse with RFC3339 format" +
        " | parse('2017-12-27T18:45:32.999999-05:00', {format = 'rfc3339'})" +
        " | 2017-12-27T18:45:32.999999-05:00",
    })
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithInstant")
    public void test_eval_shouldReturnOffsetDateTime(
        String description, String expression, OffsetDateTime expected
    ) throws Exception {
        List<?> result = client
            .eval("return require('datetime')." + expression)
            .get();

        Assertions.assertEquals(expected, result.get(0), description);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(delimiter = '|', value = {
        "Same 'compact' value" +
        " | ''" +
        " | 2023-10-25T13:55:17Z" +
        " | 2023-10-25T13:55:17Z",
        "Same 'complete' value" +
        " | ''" +
        " | 2023-10-25T13:55:17.071983+03:00" +
        " | 2023-10-25T13:55:17.071983+03:00",
        "Subtract day from 'complete' value" +
        " | :sub({day = 1})" +
        " | 2023-10-25T13:55:17.071983+03:00" +
        " | 2023-10-24T13:55:17.071983+03:00",
        "Clear timezone from 'complete' value" +
        " | :set({tz = 'UTC'})" +
        " | 2023-10-25T13:55:17.071983+03:00" +
        " | 2023-10-25T13:55:17.071983Z",
        "Clear nanoseconds from 'complete' value" +
        " | :set({nsec = 0})" +
        " | 2023-10-25T13:55:17.071983+03:00" +
        " | 2023-10-25T13:55:17+03:00",
        "Clear nanoseconds and timezone from 'complete' value" +
        " | :set({nsec = 0, tz = 'UTC'})" +
        " | 2023-10-25T13:55:17.071983+03:00" +
        " | 2023-10-25T13:55:17Z",
        "Add nanoseconds into 'compact' value" +
        " | :add({usec = 100500})" +
        " | 2023-10-25T13:55:17Z" +
        " | 2023-10-25T13:55:17.100500Z",
        "Add nanoseconds into 'complete' (has offset) value" +
        " | :add({usec = 100500})" +
        " | 2023-10-25T13:55:17+03:00" +
        " | 2023-10-25T13:55:17.100500+03:00",
        "Add nanoseconds into 'complete' (has nanos) value" +
        " | :add({usec = 100500})" +
        " | 2023-10-25T13:55:17.023067+03:00" +
        " | 2023-10-25T13:55:17.123567+03:00",
    })
    @EnabledIf("io.tarantool.driver.TarantoolUtils#versionWithInstant")
    public void test_eval_shouldHandleOffsetDateTime(
        String description, String expression, OffsetDateTime original, OffsetDateTime expected
    ) throws Exception {
        List<?> result = client
            .eval("args = {...}; return args[1]" + expression, Collections.singleton(original))
            .get();

        Assertions.assertEquals(expected, result.get(0), description);
    }

}
