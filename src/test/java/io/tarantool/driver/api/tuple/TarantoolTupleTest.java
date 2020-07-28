package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackObjectMapperException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableBooleanValueImpl;
import org.msgpack.value.impl.ImmutableDoubleValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;


public class TarantoolTupleTest {

    @Test
    void modifyTuple() {
        MessagePackValueMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();

        ImmutableArrayValue values = ValueFactory.newArray(new ImmutableDoubleValueImpl(1.0), new ImmutableLongValueImpl(50L));

        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapper);

        assertTrue(tarantoolTuple.getField(0).isPresent());
        assertEquals(1.0, tarantoolTuple.getField(0).get().getDouble());
        assertTrue(tarantoolTuple.getField(1).isPresent());
        assertEquals(50, tarantoolTuple.getField(1).get().getInteger());

        assertThrows(NoSuchElementException.class, () -> tarantoolTuple.getField(2).get().getInteger());

        //add new value
        tarantoolTuple.setField(2, ImmutableBooleanValueImpl.FALSE);
        assertTrue(tarantoolTuple.getField(2).isPresent());
        assertFalse(tarantoolTuple.getField(2).get().getBoolean());

        //replace value
        tarantoolTuple.setField(2, ImmutableBooleanValueImpl.TRUE);
        assertTrue(tarantoolTuple.getField(2).isPresent());
        assertTrue(tarantoolTuple.getField(2).get().getBoolean());

        //add new object value
        tarantoolTuple.setField(3, 3);
        assertTrue(tarantoolTuple.getField(3).isPresent());
        assertEquals(3, tarantoolTuple.getField(3).get().getInteger());

        //replace object value
        tarantoolTuple.setField(3, 15);
        assertTrue(tarantoolTuple.getField(3).isPresent());
        assertEquals(15, tarantoolTuple.getField(3).get().getInteger());

        //add new value with index more than current fields size
        tarantoolTuple.setField(5, new ImmutableDoubleValueImpl(4.4));
        tarantoolTuple.setField(7, Arrays.asList("Apple", "Ananas"));
        assertTrue(tarantoolTuple.getField(4).isPresent());
        assertNull(tarantoolTuple.getField(4).get().getInteger());

        assertTrue(tarantoolTuple.getField(6).isPresent());
        assertNull(tarantoolTuple.getField(6).get().getInteger());

        assertEquals(8, tarantoolTuple.size());

        assertEquals(4.4, tarantoolTuple.getField(5).get().getDouble());
        List<String> expectedList = new ArrayList<>();
        expectedList.add("Apple");
        expectedList.add("Ananas");
        assertEquals(expectedList, tarantoolTuple.getField(7).get().getValue(List.class));

        //add value for negative index
        assertThrows(IndexOutOfBoundsException.class, () -> tarantoolTuple.setField(-2, ImmutableBooleanValueImpl.FALSE));
        assertThrows(IndexOutOfBoundsException.class, () -> tarantoolTuple.setField(-1, 0));

        //trying to add complex object
        assertThrows(MessagePackObjectMapperException.class, () -> tarantoolTuple.setField(9, mapper));
    }
}
