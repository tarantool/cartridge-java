package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.IntegerValue;

/**
 * Default {@link IntegerValue} to {@link Integer} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultIntegerValueToIntegerConverter implements ValueConverter<IntegerValue, Integer> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Integer fromValue(IntegerValue value) {
        return value.toInt();
    }

    @Override
    public boolean canConvertValue(IntegerValue value) {
        return value.isInIntRange();
    }
}
