package io.tarantool.driver.mappers;

import org.msgpack.value.FloatValue;
import org.msgpack.value.ValueFactory;

import static java.lang.Float.MAX_VALUE;
import static java.lang.Float.MIN_VALUE;

/**
 * Default {@code Float} to {@link FloatValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultFloatConverter implements ValueConverter<FloatValue, Float>, ObjectConverter<Float, FloatValue> {

    private static final long serialVersionUID = 20210819L;

    @Override
    public FloatValue toValue(Float object) {
        return ValueFactory.newFloat(object);
    }

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
