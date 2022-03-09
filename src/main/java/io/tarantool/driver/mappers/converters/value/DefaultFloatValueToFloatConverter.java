package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.FloatValue;

import static java.lang.Float.MAX_VALUE;
import static java.lang.Float.MIN_VALUE;

/**
 * Default {@link FloatValue} to {@link Float} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultFloatValueToFloatConverter implements ValueConverter<FloatValue, Float> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Float fromValue(FloatValue value) {
        return value.toFloat();
    }

    @Override
    public boolean canConvertValue(FloatValue value) {
        double aDouble = value.toDouble();
        return aDouble <= 0.0D ? isInAcceptableRange(0.0D - aDouble) : isInAcceptableRange(aDouble);
    }

    private boolean isInAcceptableRange(double aDouble) {
        return (MIN_VALUE <= aDouble && aDouble <= MAX_VALUE) || aDouble == 0;
    }
}
