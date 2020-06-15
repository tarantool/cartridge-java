package io.tarantool.driver.mappers;

import org.msgpack.value.FloatValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code Double} to {@link FloatValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultDoubleConverter implements ValueConverter<FloatValue, Double>, ObjectConverter<Double, FloatValue> {
    @Override
    public FloatValue toValue(Double object) {
        return ValueFactory.newFloat(object);
    }

    @Override
    public Double fromValue(FloatValue value) {
        return value.toDouble();
    }
}
