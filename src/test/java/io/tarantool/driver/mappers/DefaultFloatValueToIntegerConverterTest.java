package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToIntegerConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToIntegerConverterTest {

    @Test
    public void should_convert_returnInteger() {
        //given
        DefaultFloatValueToIntegerConverter converter = new DefaultFloatValueToIntegerConverter();

        //when
        Integer actual = converter.fromValue(ValueFactory.newFloat(1));

        //then
        assertEquals(1, actual);
    }

    @Test
    public void should_convert_returnLong_ifItMaxIntegerValue() {
        //given
        DefaultFloatValueToIntegerConverter converter = new DefaultFloatValueToIntegerConverter();

        //when
        Integer actual = converter.fromValue(ValueFactory.newFloat(Integer.MAX_VALUE));

        //then
        assertEquals(Integer.MAX_VALUE, actual);
    }

    @Test
    public void should_convert_returnLong_ifItMaxIntegerValuePlusNineTenth() {
        //given
        DefaultFloatValueToIntegerConverter converter = new DefaultFloatValueToIntegerConverter();

        //when
        Integer actual = converter.fromValue(ValueFactory.newFloat(Integer.MAX_VALUE + 0.9));

        //then
        assertEquals(Integer.MAX_VALUE, actual);
    }
}
