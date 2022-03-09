package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToLongConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToLongConverterTest {

    @Test
    public void should_convert_returnLong() {
        //given
        DefaultFloatValueToLongConverter converter = new DefaultFloatValueToLongConverter();

        //when
        Long actual = converter.fromValue(ValueFactory.newFloat(1));

        //then
        assertEquals(1, actual);
    }

    @Test
    public void should_convert_returnLong_ifItMaxLongValue() {
        //given
        DefaultFloatValueToLongConverter converter = new DefaultFloatValueToLongConverter();

        //when
        Long actual = converter.fromValue(ValueFactory.newFloat(Long.MAX_VALUE));

        //then
        assertEquals(Long.MAX_VALUE, actual);
    }

    @Test
    public void should_convert_returnLong_ifItMaxLongValuePlusNineTenth() {
        //given
        DefaultFloatValueToLongConverter converter = new DefaultFloatValueToLongConverter();

        //when
        Long actual = converter.fromValue(ValueFactory.newFloat(Long.MAX_VALUE + 0.9));

        //then
        assertEquals(Long.MAX_VALUE, actual);
    }
}
