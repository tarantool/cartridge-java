package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolSpaceFieldNotFoundException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapperException;
import io.tarantool.driver.metadata.TarantoolFieldFormatMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableBooleanValueImpl;
import org.msgpack.value.impl.ImmutableDoubleValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;


public class TarantoolTupleTest {

    @Test
    void modifyTuple() {
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        MessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();
        ImmutableArrayValue values = ValueFactory.newArray(
                new ImmutableDoubleValueImpl(1.0), new ImmutableLongValueImpl(50L));

        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapper);

        assertTrue(tarantoolTuple.getField(0).isPresent());
        assertEquals(1.0, tarantoolTuple.getField(0).get().getDouble());
        assertTrue(tarantoolTuple.getField(1).isPresent());
        assertEquals(50, tarantoolTuple.getField(1).get().getInteger());

        assertThrows(NoSuchElementException.class, () -> tarantoolTuple.getField(2).get().getInteger());

        //add new field
        tarantoolTuple.setField(2, new TarantoolFieldImpl(ImmutableBooleanValueImpl.FALSE, mapper));
        assertTrue(tarantoolTuple.getField(2).isPresent());
        assertFalse(tarantoolTuple.getField(2).get().getBoolean());

        //replace field
        tarantoolTuple.setField(2, new TarantoolFieldImpl(ImmutableBooleanValueImpl.TRUE, mapper));
        assertTrue(tarantoolTuple.getField(2).isPresent());
        assertTrue(tarantoolTuple.getField(2).get().getBoolean());

        //add new object value
        tarantoolTuple.putObject(3, 3);
        assertTrue(tarantoolTuple.getField(3).isPresent());
        assertEquals(3, tarantoolTuple.getField(3).get().getInteger());

        //replace object value
        tarantoolTuple.putObject(3, 15);
        assertTrue(tarantoolTuple.getField(3).isPresent());
        assertEquals(15, tarantoolTuple.getField(3).get().getInteger());

        //add new field with index more than current fields size
        tarantoolTuple.setField(5, new TarantoolFieldImpl(new ImmutableDoubleValueImpl(4.4), mapper));
        tarantoolTuple.putObject(7, Arrays.asList("Apple", "Ananas"));
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
        assertThrows(IndexOutOfBoundsException.class,
                () -> tarantoolTuple.setField(-2,
                        new TarantoolFieldImpl(ImmutableBooleanValueImpl.FALSE, mapper)));
        assertThrows(IndexOutOfBoundsException.class, () -> tarantoolTuple.putObject(-1, 0));

        //trying to add complex object
        assertThrows(MessagePackObjectMapperException.class, () -> tarantoolTuple.putObject(9, mapper));
    }

    @Test
    void setTupleValueByName() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method setFormatMethod =
                TarantoolSpaceMetadata.class.getDeclaredMethod("setSpaceFormatMetadata", LinkedHashMap.class);
        setFormatMethod.setAccessible(true);

        TarantoolSpaceMetadata spaceMetadata = new TarantoolSpaceMetadata();

        LinkedHashMap<String, TarantoolFieldFormatMetadata> formatMetadata = new LinkedHashMap<>();
        formatMetadata.put("id", new TarantoolFieldFormatMetadata("id", "unsigned", 0));
        formatMetadata.put("book_name",
                new TarantoolFieldFormatMetadata("book_name", "string", 1));
        formatMetadata.put("author",
                new TarantoolFieldFormatMetadata("author", "string", 2));

        setFormatMethod.invoke(spaceMetadata, formatMetadata);

        ImmutableArrayValue values = ValueFactory.newArray();
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        MessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();
        TarantoolTuple tupleWithoutSpaceMetadata = new TarantoolTupleImpl(values, mapper);
        TarantoolTuple tupleWithSpaceMetadata = new TarantoolTupleImpl(values, mapper, spaceMetadata);

        assertThrows(TarantoolSpaceFieldNotFoundException.class,
                () -> tupleWithoutSpaceMetadata.setField("book_name",
                        new TarantoolFieldImpl(ValueFactory.newString("Book 1"), mapper)));
        assertThrows(TarantoolSpaceFieldNotFoundException.class,
                () -> tupleWithoutSpaceMetadata.putObject("book_name", "Book 1"));

        assertDoesNotThrow(() -> tupleWithSpaceMetadata.setField("book_name",
                new TarantoolFieldImpl(ValueFactory.newString("Book 2"), mapper)));
        assertDoesNotThrow(() -> tupleWithSpaceMetadata.putObject("author", "Book 2 author"));

        assertTrue(tupleWithSpaceMetadata.getField("book_name").isPresent());
        assertEquals("Book 2", tupleWithSpaceMetadata.getField("book_name").get().getString());

        assertTrue(tupleWithSpaceMetadata.getField("author").isPresent());
        assertEquals("Book 2 author", tupleWithSpaceMetadata.getField("author").get().getString());

        assertTrue(tupleWithSpaceMetadata.getField(2).isPresent());
        assertEquals("Book 2 author", tupleWithSpaceMetadata.getField(2).get().getString());

        //try to set field with index more than formatMetadata fields count
        assertThrows(IndexOutOfBoundsException.class, () -> tupleWithSpaceMetadata.putObject(10, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> tupleWithSpaceMetadata.putObject(3, 1));
    }
}
