package io.tarantool.driver.mappers;

import org.msgpack.value.IntegerValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code Integer} to {@link IntegerValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultIntegerConverter implements
        ValueConverter<IntegerValue, Integer>, ObjectConverter<Integer, IntegerValue> {

    private static final long serialVersionUID = 20200708L;

    @Override
    public IntegerValue toValue(Integer object) {
        return ValueFactory.newInteger(object);
    }

    @Override
    public Integer fromValue(IntegerValue value) {
        return value.asInt();
    }

    @Override
    public boolean canConvertValue(IntegerValue value) {
        return value.isInIntRange();
    }
}
