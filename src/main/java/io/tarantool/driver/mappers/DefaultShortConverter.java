package io.tarantool.driver.mappers;

import org.msgpack.value.IntegerValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link IntegerValue} to {@code Short} converter
 *
 * @author Oleg Kuznetsov
 */
public class DefaultShortConverter implements ValueConverter<IntegerValue, Short>, ObjectConverter<Short, IntegerValue> {

    @Override
    public Short fromValue(IntegerValue value) {
        return value.asShort();
    }

    @Override
    public boolean canConvertValue(IntegerValue value) {
        return value.isInShortRange();
    }

    @Override
    public IntegerValue toValue(Short object) {
        return ValueFactory.newInteger(object);
    }
}
