package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.value.DefaultIntegerValueToShortConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.*;

class DefaultShortConverterTest {

    @Test
    void should_fromValue_returnShortValue() {
        //given
        DefaultIntegerValueToShortConverter converter = new DefaultIntegerValueToShortConverter();

        //when
        Short actual = converter.fromValue(ValueFactory.newInteger(Short.MAX_VALUE));

        //then
        assertEquals(Short.MAX_VALUE, actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifItInShortRange() {
        //given
        DefaultIntegerValueToShortConverter converter = new DefaultIntegerValueToShortConverter();

        //when
        boolean actual = converter.canConvertValue(ValueFactory.newInteger(Short.MAX_VALUE));

        //then
        assertTrue(actual);
    }
}
