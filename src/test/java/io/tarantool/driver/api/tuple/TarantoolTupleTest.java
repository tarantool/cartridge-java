package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableBooleanValueImpl;
import org.msgpack.value.impl.ImmutableDoubleValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;

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

        //add new value with index more than current fields size
        tarantoolTuple.setField(4, new ImmutableDoubleValueImpl(4.4));
        assertTrue(tarantoolTuple.getField(3).isPresent());
        assertNull(tarantoolTuple.getField(3).get().getInteger());

        assertTrue(tarantoolTuple.getField(4).isPresent());
        assertEquals(4.4, tarantoolTuple.getField(4).get().getDouble());

        assertFalse(tarantoolTuple.getField(5).isPresent());
        assertFalse(tarantoolTuple.getField(6).isPresent());

        //add value for negative index
        assertThrows(IndexOutOfBoundsException.class, () -> tarantoolTuple.setField(-2, ImmutableBooleanValueImpl.FALSE));
    }
}
