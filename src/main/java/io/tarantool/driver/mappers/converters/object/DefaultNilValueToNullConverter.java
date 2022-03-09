package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.NilValue;

/**
 * Default {@link NilValue} to {@code null} converter
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public class DefaultNilValueToNullConverter implements ValueConverter<NilValue, Object> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Object fromValue(NilValue value) {
        return null;
    }

    @Override
    public boolean canConvertValue(NilValue value) {
        return value.isNilValue();
    }
}
