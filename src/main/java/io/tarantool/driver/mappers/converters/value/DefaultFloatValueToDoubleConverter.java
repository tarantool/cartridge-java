package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.FloatValue;

/**
 * Default {@link FloatValue} to {@link Double} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultFloatValueToDoubleConverter implements ValueConverter<FloatValue, Double> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Double fromValue(FloatValue value) {
        return value.toDouble();
    }
}
