package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.IntegerValue;

/**
 * Default {@link IntegerValue} to {@link Long} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultBigIntegerValueToLongConverter implements ValueConverter<IntegerValue, Long> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Long fromValue(IntegerValue value) {
        return value.toLong();
    }

    @Override
    public boolean canConvertValue(IntegerValue value) {
        return value.isInLongRange();
    }
}
