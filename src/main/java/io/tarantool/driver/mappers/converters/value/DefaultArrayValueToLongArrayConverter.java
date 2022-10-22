package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

/**
 * Default {@link ArrayValue} to {@code long[]} converter
 *
 */
public class DefaultArrayValueToLongArrayConverter implements ValueConverter<ArrayValue, long[]> {

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
        return value.list().stream().allMatch(Value::isNumberValue);
    }

}
