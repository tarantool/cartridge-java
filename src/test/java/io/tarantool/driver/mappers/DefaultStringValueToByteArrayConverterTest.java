package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.value.defaults.DefaultStringValueToByteArrayConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Artyom Dubinin
 */
public class DefaultStringValueToByteArrayConverterTest {

    @Test
    public void test_shouldConvertCorrectly() {
        //given
        DefaultStringValueToByteArrayConverter converter = new DefaultStringValueToByteArrayConverter();

        //when
        byte[] actual = converter.fromValue(ValueFactory.newString("Hello"));

        //then
        assertEquals("Hello", new String(actual));
    }
}
