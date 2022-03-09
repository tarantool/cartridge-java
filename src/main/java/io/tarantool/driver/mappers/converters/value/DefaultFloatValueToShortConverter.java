package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.FloatValue;

/**
 * Default {@link FloatValue} to {@link Short} converter
 *
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToShortConverter implements ValueConverter<FloatValue, Short> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Short fromValue(FloatValue value) {
        return value.toShort();
    }

    @Override
    public boolean canConvertValue(FloatValue value) {
        double aDouble = value.toDouble();
        return aDouble >= Short.MIN_VALUE && aDouble <= Short.MAX_VALUE;
    }
}
