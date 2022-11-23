package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.BooleanValue;

/**
 * Default {@link BooleanValue} to {@link Boolean} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultBooleanValueToBooleanConverter implements ValueConverter<BooleanValue, Boolean> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Boolean fromValue(BooleanValue value) {
        return value.getBoolean();
    }
}
