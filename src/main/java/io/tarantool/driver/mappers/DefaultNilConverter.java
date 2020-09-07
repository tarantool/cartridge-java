package io.tarantool.driver.mappers;

import org.msgpack.value.NilValue;

/**
 * Default {@link org.msgpack.value.NilValue} to null converter
 *
 * @author Sergey Volgin
 */
public class DefaultNilConverter implements ValueConverter<NilValue, Object> {

    @Override
    public Object fromValue(NilValue value) {
        return null;
    }

    @Override
    public boolean canConvertValue(NilValue value) {
        return value.isNilValue();
    }
}
