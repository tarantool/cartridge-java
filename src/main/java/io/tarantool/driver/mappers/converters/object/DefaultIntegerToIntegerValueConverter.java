package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link Integer} to {@link IntegerValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultIntegerToIntegerValueConverter implements ObjectConverter<Integer, IntegerValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public IntegerValue toValue(Integer object) {
        return ValueFactory.newInteger(object);
    }

}
