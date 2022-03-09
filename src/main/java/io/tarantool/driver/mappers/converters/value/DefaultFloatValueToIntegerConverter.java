package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.FloatValue;

/**
 * Default {@link FloatValue} to {@link Integer} converter
 *
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToIntegerConverter implements ValueConverter<FloatValue, Integer> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Integer fromValue(FloatValue value) {
        return value.toInt();
    }

    @Override
    public boolean canConvertValue(FloatValue value) {
        double aDouble = value.toDouble();
        return aDouble >= Integer.MIN_VALUE && aDouble <= Integer.MAX_VALUE;
    }
}
