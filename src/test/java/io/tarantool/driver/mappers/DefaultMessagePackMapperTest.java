package io.tarantool.driver.mappers;

import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DefaultMessagePackMapperTest {

    @Test
    void getDefaultConverter() throws MessagePackValueMapperException {
        DefaultMessagePackMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultSimpleTypeMapper();

        // check default Value converters
        assertEquals(ValueFactory.newBoolean(false), mapper.toValue(Boolean.FALSE));
        assertEquals(ValueFactory.newInteger(111), mapper.toValue(111));
        assertEquals(ValueFactory.newInteger(100500L), mapper.toValue(100500L));
        assertEquals(ValueFactory.newFloat(111.0F), mapper.toValue(111.0F));
        assertEquals(ValueFactory.newFloat(100.500D), mapper.toValue(100.500D));
        assertEquals(ValueFactory.newString("hello"), mapper.toValue("hello"));
        assertEquals(ValueFactory.newBinary(new byte[]{1, 2, 3, 4}), mapper.toValue(new byte[]{1, 2, 3, 4}));

        // check default Object converters
        assertEquals(Boolean.TRUE, mapper.fromValue(ValueFactory.newBoolean(true)));
        assertEquals(Integer.valueOf(111), mapper.fromValue(ValueFactory.newInteger(111)));
        assertEquals(Long.valueOf(4_000_000_000_000L), mapper.fromValue(ValueFactory.newInteger(4_000_000_000_000L)));
        assertEquals(Float.valueOf(111.0F), mapper.fromValue(ValueFactory.newFloat(111.0F)));
        assertEquals(Double.valueOf(Float.MAX_VALUE * 10D),
                mapper.fromValue(ValueFactory.newFloat(Float.MAX_VALUE * 10D)));
        assertEquals("hello", mapper.fromValue(ValueFactory.newString("hello")));
        assertArrayEquals(new byte[]{1, 2, 3, 4}, mapper.fromValue(ValueFactory.newBinary(new byte[]{1, 2, 3, 4})));

        // decimal
        assertEquals(BigDecimal.ONE, mapper.fromValue(mapper.toValue(BigDecimal.ONE)));

        // uuid
        UUID uuid = UUID.fromString("84b56906-aeed-11ea-b3de-0242ac130004");
        assertEquals(uuid, mapper.fromValue(mapper.toValue(uuid)));
    }

    @Test
    void testDefaultComplexConverters() {
        DefaultMessagePackMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();

        // check default complex type Value converters
        List<Object> testList = Arrays.asList("Hello", 111);
        ArrayValue expectedValue = ValueFactory.newArray(
                ValueFactory.newString("Hello"), ValueFactory.newInteger(111));
        assertEquals(expectedValue, mapper.toValue(testList));
        Map<Integer, String> testMap = new HashMap<>();
        testMap.put(1, "Hello");
        testMap.put(2, "World");
        Map<Value, Value> expectedMap = new HashMap<>();
        expectedMap.put(ValueFactory.newInteger(1), ValueFactory.newString("Hello"));
        expectedMap.put(ValueFactory.newInteger(2), ValueFactory.newString("World"));
        assertEquals(ValueFactory.newMap(expectedMap), mapper.toValue(testMap));

        // check default complex type Object converters
        ArrayValue testValue =
                ValueFactory.newArray(ValueFactory.newString("Hello"), ValueFactory.newInteger(111));
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
        DefaultMessagePackMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultSimpleTypeMapper();
        Map<Value, Value> testValue = new HashMap<>();
        CustomTuple testTuple = new CustomTuple(1234, "Test");
        testValue.put(ValueFactory.newString("id"), ValueFactory.newInteger(testTuple.getId()));
        testValue.put(ValueFactory.newString("name"), ValueFactory.newString(testTuple.getName()));
        assertThrows(MessagePackValueMapperException.class, () -> mapper.fromValue(ValueFactory.newMap(testValue)));
        mapper.registerValueConverter(MapValue.class, CustomTuple.class, v -> {
            CustomTuple tuple = new CustomTuple();
            Map<Value, Value> keyValue = v.map();
            tuple.setId(keyValue.get(ValueFactory.newString("id")).asIntegerValue().asInt());
            tuple.setName(keyValue.get(ValueFactory.newString("name")).asStringValue().asString());
            return tuple;
        });
        assertEquals(testTuple, mapper.fromValue(ValueFactory.newMap(testValue)));
    }

    @Test
    void registerObjectConverter() throws MessagePackObjectMapperException {
        DefaultMessagePackMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultSimpleTypeMapper();
        CustomTuple testTuple = new CustomTuple(1234, "Test");
        assertThrows(MessagePackObjectMapperException.class, () -> mapper.toValue(testTuple));
        mapper.registerObjectConverter(CustomTuple.class, MapValue.class, t -> {
            Map<Value, Value> keyValue = new HashMap<>();
            keyValue.put(ValueFactory.newString("id"), ValueFactory.newInteger(t.getId()));
            keyValue.put(ValueFactory.newString("name"), ValueFactory.newString(t.getName()));
            return ValueFactory.newMap(keyValue);
        });
        Map<Value, Value> testValue = new HashMap<>();
        testValue.put(ValueFactory.newString("id"), ValueFactory.newInteger(testTuple.getId()));
        testValue.put(ValueFactory.newString("name"), ValueFactory.newString(testTuple.getName()));
        assertEquals(testValue, mapper.toValue(testTuple).asMapValue().map());
    }

    private static final class CustomTuple {
        private int id;
        private String name;

        CustomTuple() {
        }

        CustomTuple(int id, String name) {
            this.id = id;
            this.name = name;
        }

        int getId() {
            return id;
        }

        void setId(int id) {
            this.id = id;
        }

        String getName() {
            return name;
        }

        void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CustomTuple that = (CustomTuple) o;
            return id == that.id &&
                    Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }
}
