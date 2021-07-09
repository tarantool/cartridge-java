package io.tarantool.driver.mappers;

import org.msgpack.value.IntegerValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code Long} to {@link IntegerValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultLongConverter implements ValueConverter<IntegerValue, Long>, ObjectConverter<Long, IntegerValue> {

    private static final long serialVersionUID = 20200708L;

    @Override
    public IntegerValue toValue(Long object) {
        return ValueFactory.newInteger(object);
    }

    @Override
    public Long fromValue(IntegerValue value) {
        return value.asLong();
    }

    @Override
    public boolean canConvertValue(IntegerValue value) {
        return value.isInLongRange();
    }
}
