package io.tarantool.driver.mappers;

import org.msgpack.value.IntegerValue;

/**
 * Default {@link IntegerValue} to {@code Float} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultIntegerValueToFloatConverter implements ValueConverter<IntegerValue, Float> {

    @Override
    public Float fromValue(IntegerValue value) {
        return value.toFloat();
    }

    @Override
    public boolean canConvertValue(IntegerValue value) {
        return value.isInIntRange();
    }
}
