package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.NilValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link org.msgpack.value.NilValue} to {@code null} converter
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public class DefaultNullToNilValueConverter implements ObjectConverter<Object, NilValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public NilValue toValue(Object o) {
        return ValueFactory.newNil();
    }
}
