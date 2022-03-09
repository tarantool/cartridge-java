package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.FloatValue;

/**
 * Default {@link FloatValue} to {@link Long} converter
 *
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToLongConverter implements ValueConverter<FloatValue, Long> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Long fromValue(FloatValue value) {
        return value.toLong();
    }

    @Override
    public boolean canConvertValue(FloatValue value) {
        double aDouble = value.toDouble();
        return aDouble >= Long.MIN_VALUE && aDouble <= Long.MAX_VALUE;
    }
}
