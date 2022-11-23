package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.core.MultiValueCallResultImpl;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;

/**
 * Converter of the stored function call result into a {@link MultiValueCallResult} with mapper inside
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class ArrayValueToMultiValueCallResultConverter<T, R extends List<T>>
    implements ValueConverter<ArrayValue, MultiValueCallResult<T, R>> {

    private static final long serialVersionUID = 20200708L;

    private final MessagePackValueMapper valueMapper;

    /**
     * Basic constructor
     *
     * @param valueMapper converter of multi-return result items to list
     */
    public ArrayValueToMultiValueCallResultConverter(MessagePackValueMapper valueMapper) {
        this.valueMapper = valueMapper;
    }

    @Override
    public MultiValueCallResult<T, R> fromValue(ArrayValue value) {
        return new MultiValueCallResultImpl<>(value, valueMapper);
    }
}
