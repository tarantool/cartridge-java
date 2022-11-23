package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.core.SingleValueCallResultImpl;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

/**
 * Converter of the stored function call result into a {@link SingleValueCallResult} with converter inside
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class ArrayValueToSingleValueCallResultSimpleConverter<T>
    implements ValueConverter<ArrayValue, SingleValueCallResult<T>> {

    private static final long serialVersionUID = 5062622344994604532L;

    private final ValueConverter<Value, T> valueConverter;

    public ArrayValueToSingleValueCallResultSimpleConverter(ValueConverter<Value, T> valueConverter) {
        this.valueConverter = valueConverter;
    }

    @Override
    public SingleValueCallResult<T> fromValue(ArrayValue value) {
        return new SingleValueCallResultImpl<>(value, valueConverter);
    }
}
