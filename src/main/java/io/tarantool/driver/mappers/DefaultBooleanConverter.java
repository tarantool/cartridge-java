package io.tarantool.driver.mappers;

import org.msgpack.value.BooleanValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code Boolean} to {@link BooleanValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultBooleanConverter implements
        ValueConverter<BooleanValue, Boolean>, ObjectConverter<Boolean, BooleanValue> {

    private static final long serialVersionUID = 20200708L;

    @Override
    public BooleanValue toValue(Boolean object) {
        return ValueFactory.newBoolean(object);
    }

    @Override
    public Boolean fromValue(BooleanValue value) {
        return value.getBoolean();
    }
}
