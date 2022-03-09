package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link Boolean} to {@link BooleanValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultBooleanToBooleanValueConverter implements ObjectConverter<Boolean, BooleanValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public BooleanValue toValue(Boolean object) {
        return ValueFactory.newBoolean(object);
    }
}
