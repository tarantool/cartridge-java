package io.tarantool.driver.mappers;

import org.msgpack.value.FloatValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code Float} to {@link FloatValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultFloatConverter implements ValueConverter<FloatValue, Float>, ObjectConverter<Float, FloatValue> {

    private static final long serialVersionUID = 20200708L;

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
        return value.toDouble() <= Float.MAX_VALUE;
    }
}
