package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link Short} to {@link IntegerValue} converter
 *
 * @author Oleg Kuznetsov
 * @author Artyom Dubinin
 */
public class DefaultShortToIntegerValueConverter implements ObjectConverter<Short, IntegerValue> {

    @Override
    public IntegerValue toValue(Short object) {
        return ValueFactory.newInteger(object);
    }
}
