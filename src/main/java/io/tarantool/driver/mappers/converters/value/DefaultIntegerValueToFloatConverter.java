package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.IntegerValue;

/**
 * Default {@link IntegerValue} to {@link Float} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultIntegerValueToFloatConverter implements ValueConverter<IntegerValue, Float> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Float fromValue(IntegerValue value) {
        return value.toFloat();
    }
}
