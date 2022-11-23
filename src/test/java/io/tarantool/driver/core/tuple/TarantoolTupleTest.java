package io.tarantool.driver.core.tuple;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.metadata.TarantoolMetadata;
import io.tarantool.driver.core.metadata.TestMetadataProvider;
import io.tarantool.driver.exceptions.TarantoolSpaceFieldNotFoundException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapperException;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableBooleanValueImpl;
import org.msgpack.value.impl.ImmutableDoubleValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TarantoolTupleTest {

    @Test
    void modifyTuple() {
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        MessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();
        ImmutableArrayValue values = ValueFactory.newArray(
            new ImmutableDoubleValueImpl(1.0), new ImmutableLongValueImpl(50L));

        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapper);

        assertTrue(tarantoolTuple.getField(0).isPresent());
        assertEquals(1.0, tarantoolTuple.getDouble(0));
        assertTrue(tarantoolTuple.getField(1).isPresent());
        assertEquals(50, tarantoolTuple.getInteger(1));

        //add new field
        tarantoolTuple.setField(2, new TarantoolFieldImpl(ImmutableBooleanValueImpl.FALSE));
        assertTrue(tarantoolTuple.getField(2).isPresent());
        assertFalse(tarantoolTuple.getField(2).get().getValue(Boolean.class, mapper));

        //replace field
        tarantoolTuple.setField(2, new TarantoolFieldImpl(ImmutableBooleanValueImpl.TRUE));
        assertTrue(tarantoolTuple.getField(2).isPresent());
        assertTrue(tarantoolTuple.getField(2).get().getValue(Boolean.class, mapper));

        //add new object value
        tarantoolTuple.putObject(3, 3);
        assertTrue(tarantoolTuple.getField(3).isPresent());
        assertEquals(3, tarantoolTuple.getInteger(3));

        //replace object value
        tarantoolTuple.putObject(3, 15);
        assertTrue(tarantoolTuple.getField(3).isPresent());
        assertEquals(15, tarantoolTuple.getInteger(3));

        //add new field with index more than current fields size
        tarantoolTuple.setField(5, new TarantoolFieldImpl(new ImmutableDoubleValueImpl(4.4)));
        tarantoolTuple.putObject(7, Arrays.asList("Apple", "Ananas"));
        assertTrue(tarantoolTuple.getField(4).isPresent());
        assertNull(tarantoolTuple.getInteger(4));

        assertTrue(tarantoolTuple.getField(6).isPresent());
        assertNull(tarantoolTuple.getInteger(6));

        assertEquals(8, tarantoolTuple.size());

        assertEquals(4.4, tarantoolTuple.getDouble(5));
        List<String> expectedList = new ArrayList<>();
        expectedList.add("Apple");
        expectedList.add("Ananas");
        assertEquals(expectedList, tarantoolTuple.getField(7).get().getValue(List.class, mapper));

        //add value for negative index
        assertThrows(IndexOutOfBoundsException.class,
            () -> tarantoolTuple.setField(-2, new TarantoolFieldImpl(ImmutableBooleanValueImpl.FALSE)));
        assertThrows(IndexOutOfBoundsException.class, () -> tarantoolTuple.putObject(-1, 0));

        //trying to add complex object
        assertThrows(MessagePackObjectMapperException.class, () -> tarantoolTuple.putObject(9, mapper));
    }

    @Test
    void setTupleValueByName() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        TarantoolMetadata testOperations = new TarantoolMetadata(new TestMetadataProvider());
        TarantoolSpaceMetadata spaceMetadata = testOperations.getSpaceByName("test").get();

        ImmutableArrayValue values = ValueFactory.newArray();
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        MessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();
        TarantoolTuple tupleWithoutSpaceMetadata = new TarantoolTupleImpl(values, mapper);
        TarantoolTuple tupleWithSpaceMetadata = new TarantoolTupleImpl(values, mapper, spaceMetadata);

        assertThrows(TarantoolSpaceFieldNotFoundException.class,
            () -> tupleWithoutSpaceMetadata.setField("book_name",
                new TarantoolFieldImpl(ValueFactory.newString("Book 1"))));
        assertThrows(TarantoolSpaceFieldNotFoundException.class,
            () -> tupleWithoutSpaceMetadata.putObject("book_name", "Book 1"));

        assertDoesNotThrow(() -> tupleWithSpaceMetadata.setField("first",
            new TarantoolFieldImpl(ValueFactory.newString("Book 2"))));
        assertDoesNotThrow(() -> tupleWithSpaceMetadata.putObject("second", 222));

        // field exists and can be converted
        assertTrue(tupleWithSpaceMetadata.canGetObject("first", String.class));
        // mapper does not have converter StringValue -> Integer
        assertFalse(tupleWithSpaceMetadata.canGetObject("first", Integer.class));
        // non-existing field
        assertFalse(tupleWithSpaceMetadata.canGetObject("non_existing", Integer.class));

        assertTrue(tupleWithSpaceMetadata.getField("first").isPresent());
        assertEquals("Book 2", tupleWithSpaceMetadata.getString("first"));

        assertTrue(tupleWithSpaceMetadata.getField("second").isPresent());
        assertEquals(222, tupleWithSpaceMetadata.getInteger("second"));

        assertTrue(tupleWithSpaceMetadata.getField(1).isPresent());
        assertEquals(222, tupleWithSpaceMetadata.getInteger(1));

        //try to set field with index more than formatMetadata fields count
        assertThrows(IndexOutOfBoundsException.class, () -> tupleWithSpaceMetadata.putObject(10, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> tupleWithSpaceMetadata.putObject(5, 1));
    }

    @Test
    void convertStructures() {
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        MessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();

        List<Object> testList = new ArrayList<>();
        testList.add("Apple");
        testList.add(123456);
        Map<String, String> nestedMap = Collections.singletonMap("key", "value");
        List<Object> nestedList = Arrays.asList("lol", nestedMap);
        testList.add(nestedList);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(testList, mapper);

        Value value = tarantoolTuple.toMessagePackValue(mapper);
        TarantoolTuple convertedTuple = new TarantoolTupleImpl(value.asArrayValue(), mapper);

        // field exists and can be converted
        assertTrue(convertedTuple.canGetObject(0, String.class));
        // mapper does not have converter StringValue -> Integer
        assertFalse(convertedTuple.canGetObject(0, Integer.class));
        // non-existing field
        assertFalse(convertedTuple.canGetObject(10, Integer.class));

        assertEquals("Apple", convertedTuple.getString(0));
        assertEquals(String.class, convertedTuple.getObject(0).get().getClass());
        assertEquals(123456, convertedTuple.getInteger(1));
        assertEquals(123456L, convertedTuple.getLong(1));
        assertEquals(Integer.class, convertedTuple.getObject(1).get().getClass());
        List<?> resultList = convertedTuple.getList(2);
        assertEquals(ArrayList.class, convertedTuple.getObject(2).get().getClass());
        assertEquals("lol", resultList.get(0));
        assertEquals(nestedMap, resultList.get(1));
    }

    @Test
    void serializeTuple() throws IOException, ClassNotFoundException {
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        MessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();
        TarantoolMetadata testOperations = new TarantoolMetadata(new TestMetadataProvider());
        TarantoolSpaceMetadata spaceMetadata = testOperations.getSpaceByName("test").get();

        List<Object> testList = new ArrayList<>();
        testList.add("Apple");
        testList.add(123456);
        Map<String, String> nestedMap = Collections.singletonMap("key", "value");
        List<Object> nestedList = Arrays.asList("lol", nestedMap);
        testList.add(nestedList);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(testList, mapper, spaceMetadata);

        byte[] buffer;
        int len = 4096;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(len);
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(tarantoolTuple);
            oos.flush();
            buffer = bos.toByteArray();
        }
        TarantoolTuple serializedTuple;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            serializedTuple = (TarantoolTuple) ois.readObject();
        }

        assertEquals(tarantoolTuple.size(), serializedTuple.size());
        // Here not only the fields are checked,
        // but the presence of space metadata and the encapsulated mappers too
        assertEquals(testList.get(0), serializedTuple.getString(0));
        assertEquals(testList.get(1), serializedTuple.getInteger("second"));
        assertEquals(testList.get(2), serializedTuple.getList(2));
    }
}
