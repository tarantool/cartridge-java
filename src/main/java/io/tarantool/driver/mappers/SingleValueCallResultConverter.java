package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.SingleValueCallResultImpl;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

/**
 * Converter of the stored function call result into a {@link SingleValueCallResult}
 *
 * @author Alexey Kuzin
 */
public class SingleValueCallResultConverter<T> implements ValueConverter<ArrayValue, SingleValueCallResult<T>> {

    private final ValueConverter<Value, T> valueConverter;

    public SingleValueCallResultConverter(ValueConverter<Value, T> valueConverter) {
        this.valueConverter = valueConverter;
    }

    @Override
    public SingleValueCallResult<T> fromValue(ArrayValue value) {
        return new SingleValueCallResultImpl<>(value, valueConverter);
    }
}
