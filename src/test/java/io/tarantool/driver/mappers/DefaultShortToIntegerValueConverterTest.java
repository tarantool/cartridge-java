package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.object.DefaultShortToIntegerValueConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Andrey Klyuev
 */
public class DefaultShortToIntegerValueConverterTest {

    @Test
    void toValue() {
        //given
        DefaultShortToIntegerValueConverter converter = new DefaultShortToIntegerValueConverter();

        //when
        IntegerValue actual = converter.toValue(Short.valueOf("1"));

        //then
        assertEquals(ValueFactory.newInteger(Short.valueOf("1")), actual);
    }
}
