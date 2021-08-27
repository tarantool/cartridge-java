package io.tarantool.driver.mappers;

import org.msgpack.value.FloatValue;

/**
 * Default {@link FloatValue} to {@code Long} converter
 *
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToLongConverter implements ValueConverter<FloatValue, Long> {

    private static final long serialVersionUID = 20210819L;

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
