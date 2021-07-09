package io.tarantool.driver.mappers;

import org.msgpack.value.NilValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link org.msgpack.value.NilValue} to null converter
 *
 * @author Sergey Volgin
 */
public class DefaultNilConverter implements ValueConverter<NilValue, Object>, ObjectConverter<Object, NilValue> {

    private static final long serialVersionUID = 20200708L;

    @Override
    public Object fromValue(NilValue value) {
        return null;
    }

    @Override
    public NilValue toValue(Object o) {
        return ValueFactory.newNil();
    }

    @Override
    public boolean canConvertValue(NilValue value) {
        return value.isNilValue();
    }
}
