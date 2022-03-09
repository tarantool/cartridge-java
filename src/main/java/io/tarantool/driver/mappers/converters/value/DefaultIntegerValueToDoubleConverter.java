package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.IntegerValue;

/**
 * Default {@link IntegerValue} to {@link Double} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultIntegerValueToDoubleConverter implements ValueConverter<IntegerValue, Double> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Double fromValue(IntegerValue value) {
        return value.toDouble();
    }
}
