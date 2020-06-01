package io.tarantool.driver.mappers;

import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultMessagePackObjectMapperTest {

    @Test
    void getInstance() throws MessagePackValueMapperException {
        DefaultMessagePackObjectMapper mapper = DefaultMessagePackObjectMapper.getInstance();

        // check default Value converters
        assertEquals(ValueFactory.newInteger(111), mapper.toValue(111));
        assertEquals(ValueFactory.newInteger(100500L), mapper.toValue(100500L));
        assertEquals(ValueFactory.newString("hello"), mapper.toValue("hello"));
        assertEquals(ValueFactory.newBinary(new byte[]{1,2,3,4}), mapper.toValue(new byte[]{1,2,3,4}));

        // check default complex type Value converters
        List<Object> testList = Arrays.asList("Hello", 111);
        ArrayValue expectedValue = ValueFactory.newArray(ValueFactory.newString("Hello"), ValueFactory.newInteger(111));
        assertEquals(expectedValue, mapper.toValue(testList));
        Map<Integer, String> testMap = new HashMap<>();
        testMap.put(1, "Hello");
        testMap.put(2, "World");
        Map<Value, Value> expectedMap = new HashMap<>();
        expectedMap.put(ValueFactory.newInteger(1), ValueFactory.newString("Hello"));
        expectedMap.put(ValueFactory.newInteger(2), ValueFactory.newString("World"));
        assertEquals(ValueFactory.newMap(expectedMap), mapper.toValue(testMap));

        // check default Object converters
        assertEquals(Integer.valueOf(111), mapper.fromValue(ValueFactory.newInteger(111)));
        assertEquals(Long.valueOf(4_000_000_000_000L), mapper.fromValue(ValueFactory.newInteger(4_000_000_000_000L)));
        assertEquals("hello", mapper.fromValue(ValueFactory.newString("hello")));
        assertArrayEquals(new byte[]{1,2,3,4}, (byte[]) mapper.fromValue(ValueFactory.newBinary(new byte[]{1,2,3,4})));

        // check default complex type Object converters
        ArrayValue testValue = ValueFactory.newArray(ValueFactory.newString("Hello"), ValueFactory.newInteger(111));
        List<Object> expectedList = Arrays.asList("Hello", 111);
        assertEquals(expectedList, mapper.fromValue(testValue));
        Map<Value, Value> testMap1 = new HashMap<>();
        expectedMap.put(ValueFactory.newInteger(1), ValueFactory.newString("Hello"));
        expectedMap.put(ValueFactory.newInteger(2), ValueFactory.newString("World"));
        Map<Integer, String> expectedMap1 = new HashMap<>();
        testMap.put(1, "Hello");
        testMap.put(2, "World");
        assertEquals(expectedMap1, mapper.fromValue(ValueFactory.newMap(testMap1)));

        // what about array of arrays?
        List<List<Object>> complexList = Arrays.asList(Arrays.asList("Hello", 111), Arrays.asList("World", 222));
        ArrayValue complexValue = ValueFactory.newArray(
                ValueFactory.newArray(ValueFactory.newString("Hello"), ValueFactory.newInteger(111)),
                ValueFactory.newArray(ValueFactory.newString("World"), ValueFactory.newInteger(222)));
        assertEquals(complexValue, mapper.toValue(complexList));
        assertEquals(complexList, mapper.fromValue(complexValue));

        // map of maps
        Map<Integer, Map<Integer, String>> complexMap = new HashMap<>();
        complexMap.put(1, new HashMap<>());
        complexMap.get(1).put(3, "Hello");
        complexMap.get(1).put(4, "World");
        complexMap.put(2, new HashMap<>());
        complexMap.get(2).put(5, "It's Wednesday");
        complexMap.get(2).put(6, "My dudes");
        assertEquals(complexMap, mapper.fromValue(mapper.toValue(complexMap)));
    }

    @Test
    void registerValueConverter() throws MessagePackValueMapperException {
        DefaultMessagePackObjectMapper mapper = DefaultMessagePackObjectMapper.getInstance();
        assertThrows(MessagePackValueMapperException.class, () -> mapper.fromValue(ValueFactory.newBoolean(true)));
        mapper.registerValueConverter(BooleanValue.class, BooleanValue::getBoolean);
        assertEquals(mapper.fromValue(ValueFactory.newBoolean(false)), Boolean.FALSE);
    }

    @Test
    void registerObjectConverter() throws MessagePackValueMapperException {
        DefaultMessagePackObjectMapper mapper = DefaultMessagePackObjectMapper.getInstance();
        assertThrows(MessagePackValueMapperException.class, () -> mapper.toValue(Boolean.TRUE));
        mapper.registerObjectConverter(Boolean.class, ValueFactory::newBoolean);
        assertEquals(mapper.toValue(Boolean.FALSE), ValueFactory.newBoolean(false));
    }
}