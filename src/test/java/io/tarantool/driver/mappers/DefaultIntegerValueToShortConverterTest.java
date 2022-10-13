package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.value.DefaultIntegerValueToShortConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Andrey Klyuev
 */
public class DefaultIntegerValueToShortConverterTest {

    @Test
    public void test_shouldConvertCorrectly() {
        //given
        DefaultIntegerValueToShortConverter converter = new DefaultIntegerValueToShortConverter();

        //when
        Short actual = converter.fromValue(ValueFactory.newInteger(new Short("1")));

        //then
        assertEquals(new Short("1"), actual);
    }
}
