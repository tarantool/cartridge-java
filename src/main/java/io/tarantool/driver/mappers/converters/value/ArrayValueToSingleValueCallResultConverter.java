package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.core.SingleValueCallResultImpl;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Converter of the stored function call result into a {@link SingleValueCallResult} with mapper inside
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class ArrayValueToSingleValueCallResultConverter<T>
    implements ValueConverter<ArrayValue, SingleValueCallResult<T>> {

    private static final long serialVersionUID = 7218783308373068532L;

    private final MessagePackValueMapper valueMapper;

    public ArrayValueToSingleValueCallResultConverter(MessagePackValueMapper valueMapper) {
        this.valueMapper = valueMapper;
    }

    @Override
    public SingleValueCallResult<T> fromValue(ArrayValue value) {
        return new SingleValueCallResultImpl<>(value, valueMapper);
    }
}
