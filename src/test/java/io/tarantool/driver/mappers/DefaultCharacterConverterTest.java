package io.tarantool.driver.mappers;

import io.tarantool.driver.api.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import org.junit.jupiter.api.Test;
import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DefaultCharacterConverterTest {

    @Test
    void test_toValue_shouldReturnStringValue_s() {
        DefaultCharacterConverter converter = new DefaultCharacterConverter();
        StringValue result = converter.toValue('s');
        assertEquals(ValueFactory.newString("s"), result);
    }

    @Test
    void test_fromValue_shouldReturnCharacter_s() {
        DefaultCharacterConverter converter = new DefaultCharacterConverter();
        Character result = converter.fromValue(ValueFactory.newString("s"));
        assertEquals('s', result);
    }

    @Test
    void test_canConvertValue_shouldReturnTrue_ifStringConsistsOfOneSymbol() {
        DefaultCharacterConverter converter = new DefaultCharacterConverter();
        assertTrue(converter.canConvertValue(ValueFactory.newString("s")));
        assertFalse(converter.canConvertValue(ValueFactory.newString("string")));
        assertFalse(converter.canConvertValue(ValueFactory.newString("")));
    }

    @Test
    void test_characterConverter_shouldReturnProperCharacter_ifIndexExists() {
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        TarantoolTupleFactory tupleFactory =
                new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

        TarantoolTuple tarantoolTuple = tupleFactory.create(ValueFactory.newString("a"), ValueFactory.newString("б"));
        assertEquals('a', tarantoolTuple.getCharacter(0)); // Latin letter
        assertEquals('б', tarantoolTuple.getCharacter(1)); // Cyrillic letter
        assertNull(tarantoolTuple.getCharacter(10));
        assertNull(tarantoolTuple.getCharacter("nonexistentFieldName"));

        assertEquals('a', tarantoolTuple.getObject(0, Character.class).get());
        assertEquals('б', tarantoolTuple.getObject(1, Character.class).get());
        assertEquals(Optional.empty(), tarantoolTuple.getObject(10, Character.class));
        assertEquals(Optional.empty(), tarantoolTuple.getObject("nonexistentFieldName", Character.class));
    }
}
