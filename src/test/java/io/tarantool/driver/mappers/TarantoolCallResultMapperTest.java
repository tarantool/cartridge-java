package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.exceptions.TarantoolServerInternalException;
import io.tarantool.driver.exceptions.TarantoolTupleConversionException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TarantoolCallResultMapperTest {

    private static final MessagePackMapper defaultMapper =
            DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private final DefaultResultMapperFactoryFactory mapperFactoryFactory =
            new DefaultResultMapperFactoryFactory();
    private final
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
            defaultResultMapper = mapperFactoryFactory.defaultTupleSingleResultMapperFactory()
            .withDefaultTupleValueConverter(defaultMapper, null);

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
    void testSingleValueCallResultMapper() {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        DefaultResultMapperFactoryFactory mapperFactoryFactory = new DefaultResultMapperFactoryFactory();
        CallResultMapper<TarantoolResult<TarantoolTuple>,
                SingleValueCallResult<TarantoolResult<TarantoolTuple>>> mapper =
                mapperFactoryFactory.defaultTupleSingleResultMapperFactory()
                        .withDefaultTupleValueConverter(defaultMapper, null);

        //[nil, message]
        ArrayValue errorResult = ValueFactory.newArray(ValueFactory.newNil(), ValueFactory.newString("ERROR"));
        assertThrows(TarantoolServerInternalException.class, () -> mapper.fromValue(errorResult));

        //[nil, {str=message, stack=stacktrace}]
        MapValue error = ValueFactory.newMap(
                ValueFactory.newString("str"),
                ValueFactory.newString("ERROR"),
                ValueFactory.newString("stack"),
                ValueFactory.newString("stacktrace")
        );
        ArrayValue errorResult1 = ValueFactory.newArray(ValueFactory.newNil(), error);
        assertThrows(TarantoolServerInternalException.class, () -> mapper.fromValue(errorResult1));

        //[[[],...]]
        List<Object> nestedList1 = Arrays.asList("nested", "array", 1);
        TarantoolTuple tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        List<Object> nestedList2 = Arrays.asList("nested", "array", 2);
        TarantoolTuple tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));
        ArrayValue callResult = ValueFactory.newArray(testTuples);
        SingleValueCallResult<TarantoolResult<TarantoolTuple>> result = mapper.fromValue(callResult);
        TarantoolResult<TarantoolTuple> tuples = result.value();

        assertEquals(2, tuples.size());
        assertEquals("abc", tuples.get(0).getString(0));
        assertEquals(1234, tuples.get(0).getInteger(1));
        assertEquals(nestedList1, tuples.get(0).getList(2));
        assertEquals("def", tuples.get(1).getString(0));
        assertEquals(5678, tuples.get(1).getInteger(1));
        assertEquals(nestedList2, tuples.get(1).getList(2));
    }

    @Test
    void testDefaultTarantoolTupleResponse_singleResultShouldThrowException() {
        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        assertThrows(TarantoolTupleConversionException.class, () -> defaultResultMapper.fromValue(testTuples));
    }

    @Test
    void testMultiValueCallResultMapper() {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        DefaultResultMapperFactoryFactory mapperFactoryFactory = new DefaultResultMapperFactoryFactory();
        CallResultMapper<TarantoolResult<TarantoolTuple>,
                MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>>> mapper =
                mapperFactoryFactory.defaultTupleMultiResultMapperFactory()
                        .withDefaultTupleValueConverter(defaultMapper, null);

        //[[], ...]
        List<Object> nestedList1 = Arrays.asList("nested", "array", 1);
        TarantoolTuple tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        List<Object> nestedList2 = Arrays.asList("nested", "array", 2);
        TarantoolTuple tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>> result = mapper.fromValue(testTuples);
        TarantoolResult<TarantoolTuple> tuples = result.value();

        assertEquals(2, tuples.size());
        assertEquals("abc", tuples.get(0).getString(0));
        assertEquals(1234, tuples.get(0).getInteger(1));
        assertEquals(nestedList1, tuples.get(0).getList(2));
        assertEquals("def", tuples.get(1).getString(0));
        assertEquals(5678, tuples.get(1).getInteger(1));
        assertEquals(nestedList2, tuples.get(1).getList(2));
    }

    @Test
    void testResponseWithError() {
        ArrayValue resultWithError = ValueFactory.newArray(
                ValueFactory.newNil(), ValueFactory.newString("Error message from server")
        );

        TarantoolServerInternalException e = assertThrows(TarantoolServerInternalException.class,
                () -> defaultResultMapper.fromValue(resultWithError));
        assertEquals("Error message from server", e.getMessage());
    }

    @Test
    void testNilResponse() {
        ArrayValue nilResult = ValueFactory.newArray(ValueFactory.newNil());

        SingleValueCallResult<TarantoolResult<TarantoolTuple>> result = defaultResultMapper.fromValue(nilResult);

        assertNull(result.value());
    }

    @Test
    void testNotUnpackedTable() {
        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));

        //[[[], [], ...]]
        ArrayValue resultNotUnpacked = ValueFactory.newArray(testTuples);

        SingleValueCallResult<TarantoolResult<TarantoolTuple>> result =
                defaultResultMapper.fromValue(resultNotUnpacked);
        TarantoolResult<TarantoolTuple> tuples = result.value();
        assertEquals(2, tuples.size());
        assertEquals("abc", tuples.get(0).getString(0));
        assertEquals(1234, tuples.get(0).getInteger(1));
        assertEquals(nestedList1, tuples.get(0).getList(2));
        assertEquals("def", tuples.get(1).getString(0));
        assertEquals(5678, tuples.get(1).getInteger(1));
        assertEquals(nestedList2, tuples.get(1).getList(2));
    }
}
