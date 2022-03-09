package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link Long} to {@link IntegerValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultLongToIntegerValueConverter implements ObjectConverter<Long, IntegerValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public IntegerValue toValue(Long object) {
        return ValueFactory.newInteger(object);
    }

}
