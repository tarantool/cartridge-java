package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToFloatConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ValueFactory;

import static java.lang.Float.MAX_VALUE;
import static java.lang.Float.MIN_VALUE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultFloatConverterTest {

    @Test
    void should_canConvertValue_returnTrue_ifFloatIsMinusZero() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(-0.0));

        //then
        assertTrue(actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifFloatIsZeroWithFloatingPoint() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(0.0));

        //then
        assertTrue(actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifFloatIsZero() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(0));

        //then
        assertTrue(actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifCheckFloatMinValue() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(MIN_VALUE));

        //then
        assertTrue(actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifCheckFloatMaxValue() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(MAX_VALUE));

        //then
        assertTrue(actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifCheckFloatOne() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(1.0f));

        //then
        assertTrue(actual);
    }

    @Test
    void should_canConvertValue_returnTrue_ifCheckDoubleOne() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(1.0d));

        //then
        assertTrue(actual);
    }

    @Test
    void should_canConvertValue_returnFalse_ifCheckDoubleMinValue() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(Double.MIN_VALUE));

        //then
        assertFalse(actual);
    }

    @Test
    void should_canConvertValue_returnFalse_ifCheckDoubleMaxValue() {
        //given
        DefaultFloatValueToFloatConverter defaultFloatConverter = new DefaultFloatValueToFloatConverter();

        //when
        boolean actual = defaultFloatConverter.canConvertValue(ValueFactory.newFloat(Double.MAX_VALUE));

        //then
        assertFalse(actual);
    }
}
