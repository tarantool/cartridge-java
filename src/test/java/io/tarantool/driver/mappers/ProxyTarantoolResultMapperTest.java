package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ValueFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProxyTarantoolResultMapperTest {

    private static final MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance()
            .defaultComplexTypesMapper();
    private static final TarantoolResultMapperFactory mapperFactory = new TarantoolResultMapperFactory();

    private static List<Object> nestedList1;
    private static TarantoolTuple tupleOne;
    private static List<Object> nestedList2;
    private static TarantoolTuple tupleTwo;

    @BeforeAll
    public static void setUp() {
        nestedList1 = Arrays.asList("nested", "array", 1);
        tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        nestedList2 = Arrays.asList("nested", "array", 2);
        tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
    }

    @Test
    void testDefaultTarantoolTupleResponse() {
        ProxyTarantoolResultMapper<TarantoolTuple> proxyMapper = mapperFactory
                .withProxyConverter(mapperFactory.getDefaultTupleValueConverter(defaultMapper));

        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        TarantoolResult<TarantoolTuple> result = proxyMapper.fromValue(testTuples);
        assertEquals(2, result.size());
        assertEquals("abc", result.get(0).getString(0));
        assertEquals(1234, result.get(0).getInteger(1));
        assertEquals(nestedList1, result.get(0).getList(2));
        assertEquals("def", result.get(1).getString(0));
        assertEquals(5678, result.get(1).getInteger(1));
        assertEquals(nestedList2, result.get(1).getList(2));
    }

    @Test
    void testResponseWithError() {
        ProxyTarantoolResultMapper<TarantoolTuple> proxyMapper = mapperFactory
                .withProxyConverter(mapperFactory.getDefaultTupleValueConverter(defaultMapper));

        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        ArrayValue resultWithNilError = ValueFactory.newArray(
                testTuples, ValueFactory.newNil()
        );

        TarantoolResult<TarantoolTuple> result = proxyMapper.fromValue(resultWithNilError);
        assertEquals(2, result.size());
        assertEquals("abc", result.get(0).getString(0));
        assertEquals(1234, result.get(0).getInteger(1));
        assertEquals(nestedList1, result.get(0).getList(2));
        assertEquals("def", result.get(1).getString(0));
        assertEquals(5678, result.get(1).getInteger(1));
        assertEquals(nestedList2, result.get(1).getList(2));

        ArrayValue resultWithError = ValueFactory.newArray(
                ValueFactory.newNil(), ValueFactory.newString("Error message from server")
        );

        TarantoolSpaceOperationException e = assertThrows(TarantoolSpaceOperationException.class,
                () -> proxyMapper.fromValue(resultWithError));
        assertEquals("Proxy operation error: Error message from server", e.getMessage());
    }

    @Test
    void testNilResponse() {
        ProxyTarantoolResultMapper<TarantoolTuple> proxyMapper = mapperFactory
                .withProxyConverter(mapperFactory.getDefaultTupleValueConverter(defaultMapper));

        ArrayValue nilResult = ValueFactory.newArray(ValueFactory.newNil());

        TarantoolResult<TarantoolTuple> result = proxyMapper.fromValue(nilResult);

        assertEquals(0, result.size());
    }

    @Test
    void testNotUnpackedTable() {
        ProxyTarantoolResultMapper<TarantoolTuple> proxyMapper = mapperFactory
                .withProxyConverter(mapperFactory.getDefaultTupleValueConverter(defaultMapper));

        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        ArrayValue resultNotUnpacked = ValueFactory.newArray(testTuples);

        TarantoolResult<TarantoolTuple> result = proxyMapper.fromValue(resultNotUnpacked);
        assertEquals(2, result.size());
        assertEquals("abc", result.get(0).getString(0));
        assertEquals(1234, result.get(0).getInteger(1));
        assertEquals(nestedList1, result.get(0).getList(2));
        assertEquals("def", result.get(1).getString(0));
        assertEquals(5678, result.get(1).getInteger(1));
        assertEquals(nestedList2, result.get(1).getList(2));
    }
}
