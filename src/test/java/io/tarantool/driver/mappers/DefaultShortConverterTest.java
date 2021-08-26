package io.tarantool.driver.mappers;

import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.*;

class DefaultShortConverterTest {

    @Test
    void should_fromValue_returnShortValue() {
        //given
        DefaultShortConverter converter = new DefaultShortConverter();

        //when
        Short actual = converter.fromValue(ValueFactory.newInteger(Short.MAX_VALUE));

        //then
        assertEquals(Short.MAX_VALUE, actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifItInShortRange() {
        //given
        DefaultShortConverter converter = new DefaultShortConverter();

        //when
        boolean actual = converter.canConvertValue(ValueFactory.newInteger(Short.MAX_VALUE));

        //then
        assertTrue(actual);
    }
}
