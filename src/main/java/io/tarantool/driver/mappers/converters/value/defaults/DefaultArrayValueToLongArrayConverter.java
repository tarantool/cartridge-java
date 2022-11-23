package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Default {@link ArrayValue} to {@code long[]} converter
 *
 * @author Anastasiia Romanova
 */
public class DefaultArrayValueToLongArrayConverter implements ValueConverter<ArrayValue, long[]> {

    private static final long serialVersionUID = 20221022L;

    @Override
    public long[] fromValue(ArrayValue value) {
        long[] values = new long[value.size()];
        for (int i = 0; i < value.size(); i++) {
            values[i] = value.list().get(i).asNumberValue().toLong();
        }
        return values;
    }

    @Override
    public boolean canConvertValue(ArrayValue value) {
        if (value != null) {
            if (value.size() > 0) {
                return value.get(0).isNumberValue();
            }
            return true;
        }
        return false;
    }

}
