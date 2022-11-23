package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.value.defaults.DefaultFloatValueToShortConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToShortConverterTest {

    @Test
    public void should_convert_returnShort() {
        //given
        DefaultFloatValueToShortConverter converter = new DefaultFloatValueToShortConverter();

        //when
        Short actual = converter.fromValue(ValueFactory.newFloat(1));

        //then
        assertEquals(Short.valueOf("1"), actual);
    }

    @Test
    public void should_convert_returnLong_ifItMaxShortValue() {
        //given
        DefaultFloatValueToShortConverter converter = new DefaultFloatValueToShortConverter();

        //when
        Short actual = converter.fromValue(ValueFactory.newFloat(Short.MAX_VALUE));

        //then
        assertEquals(Short.MAX_VALUE, actual);
    }

    @Test
    public void should_convert_returnLong_ifItMaxShortValuePlusNineTenth() {
        //given
        DefaultFloatValueToShortConverter converter = new DefaultFloatValueToShortConverter();

        //when
        Short actual = converter.fromValue(ValueFactory.newFloat(Short.MAX_VALUE + 0.9));

        //then
        assertEquals(Short.MAX_VALUE, actual);
    }
}
