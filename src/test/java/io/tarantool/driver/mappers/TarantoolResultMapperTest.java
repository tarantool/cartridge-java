package io.tarantool.driver.mappers;

import io.tarantool.driver.CustomTuple;
import io.tarantool.driver.CustomTupleResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleResult;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.factories.RowsMetadataStructureToTarantoolResultMapperFactory;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactoryImpl;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactory;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TarantoolResultMapperTest {
    @Test
    void testWithTarantoolTuple() {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        ResultMapperFactoryFactoryImpl
            mapperFactoryFactory = new ResultMapperFactoryFactoryImpl();
        TarantoolResultMapper<TarantoolTuple> mapper = mapperFactoryFactory
            .tupleResultMapperFactory().withFlattenTupleMapper(defaultMapper, null);
        List<Object> nestedList1 = Arrays.asList("nested", "array", 1);
        TarantoolTuple tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        List<Object> nestedList2 = Arrays.asList("nested", "array", 2);
        TarantoolTuple tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
        ArrayValue testTuples = ValueFactory.newArray(
            tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));
        TarantoolResult<TarantoolTuple> result = mapper.fromValue(testTuples, TarantoolTupleResult.class);
        assertEquals(2, result.size());
        assertEquals("abc", result.get(0).getString(0));
        assertEquals(1234, result.get(0).getInteger(1));
        assertEquals(nestedList1, result.get(0).getList(2));
        assertEquals("def", result.get(1).getString(0));
        assertEquals(5678, result.get(1).getInteger(1));
        assertEquals(nestedList2, result.get(1).getList(2));
    }

    @Test
    void testWithCustomTuple() {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        defaultMapper.registerObjectConverter(CustomTuple.class, ArrayValue.class, t ->
            ValueFactory.newArray(ValueFactory.newInteger(t.getId()), ValueFactory.newString(t.getName())));
        ResultMapperFactoryFactory mapperFactoryFactory = new ResultMapperFactoryFactoryImpl();
        RowsMetadataStructureToTarantoolResultMapperFactory<CustomTuple> mapperFactory
            = mapperFactoryFactory.rowsMetadataStructureResultMapperFactory();
        TarantoolResultMapper<CustomTuple> mapper = mapperFactory.withArrayValueToTarantoolResultConverter(v -> {
            CustomTuple tuple = new CustomTuple();
            List<Value> values = v.list();
            tuple.setId(values.get(0).asIntegerValue().asInt());
            tuple.setName(values.get(1).asStringValue().asString());
            return tuple;
        });
        CustomTuple tupleOne = new CustomTuple(1, "abcd");
        CustomTuple tupleTwo = new CustomTuple(2, "efgh");
        ArrayValue testTuples =
            ValueFactory.newArray(Arrays.asList(defaultMapper.toValue(tupleOne), defaultMapper.toValue(tupleTwo)));
        TarantoolResult<CustomTuple> result = mapper.fromValue(testTuples, CustomTupleResult.class);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).getId());
        assertEquals("abcd", result.get(0).getName());
        assertEquals(2, result.get(1).getId());
        assertEquals("efgh", result.get(1).getName());
    }
}
