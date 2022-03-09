package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.IntegerValue;

/**
 * Default {@link IntegerValue} to {@link Short} converter
 *
 * @author Oleg Kuznetsov
 * @author Artyom Dubinin
 */
public class DefaultIntegerValueToShortConverter implements ValueConverter<IntegerValue, Short> {

    @Override
    public Short fromValue(IntegerValue value) {
        return value.toShort();
    }

    @Override
    public boolean canConvertValue(IntegerValue value) {
        return value.isInShortRange();
    }
}
