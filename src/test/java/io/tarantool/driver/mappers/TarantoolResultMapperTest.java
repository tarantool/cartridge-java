package io.tarantool.driver.mappers;

import io.tarantool.driver.CustomTuple;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolField;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TarantoolResultMapperTest {
    @Test
    void testWithTarantoolTuple() {
        TarantoolResultMapperFactory mapperFactory = new TarantoolResultMapperFactory();
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolResultMapper<TarantoolTuple> mapper =
                mapperFactory.withConverter(mapperFactory.getDefaultTupleValueConverter(defaultMapper));
        List<Object> nestedList1 = Arrays.asList("nested", "array", 1);
        TarantoolTuple tupleOne = new TarantoolTupleImpl(Arrays.asList("abc", 1234, nestedList1), defaultMapper);
        List<Object> nestedList2 = Arrays.asList("nested", "array", 2);
        TarantoolTuple tupleTwo = new TarantoolTupleImpl(Arrays.asList("def", 5678, nestedList2), defaultMapper);
        ArrayValue testTuples = ValueFactory.newArray(
                tupleOne.toMessagePackValue(defaultMapper), tupleTwo.toMessagePackValue(defaultMapper));
        TarantoolResult<TarantoolTuple> result = mapper.fromValue(testTuples);
        assertEquals(2, result.size());
        assertEquals("abc", result.get(0).getField(0).map(TarantoolField::getString).get());
        assertEquals(1234, result.get(0).getField(1).map(TarantoolField::getInteger).get());
        assertEquals(nestedList1, result.get(0).getField(2).map(f -> f.getValue(ArrayList.class)).get());
        assertEquals("def", result.get(1).getField(0).map(TarantoolField::getString).get());
        assertEquals(5678, result.get(1).getField(1).map(TarantoolField::getInteger).get());
        assertEquals(nestedList2, result.get(1).getField(2).map(f -> f.getValue(ArrayList.class)).get());
    }

    @Test
    void testWithCustomTuple() {
        TarantoolResultMapperFactory mapperFactory = new TarantoolResultMapperFactory();
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        defaultMapper.registerObjectConverter(CustomTuple.class, ArrayValue.class, t ->
                ValueFactory.newArray(ValueFactory.newInteger(t.getId()), ValueFactory.newString(t.getName())));
        TarantoolResultMapper<CustomTuple> mapper = mapperFactory.withConverter(CustomTuple.class, v -> {
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
        TarantoolResult<CustomTuple> result = mapper.fromValue(testTuples);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).getId());
        assertEquals("abcd", result.get(0).getName());
        assertEquals(2, result.get(1).getId());
        assertEquals("efgh", result.get(1).getName());
    }
}
