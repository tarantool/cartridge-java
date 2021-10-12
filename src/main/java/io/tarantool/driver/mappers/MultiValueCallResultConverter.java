package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.core.MultiValueCallResultImpl;
import org.msgpack.value.ArrayValue;

import java.util.List;

/**
 * Converter for stored function call result to a list of items
 *
 * @author Alexey Kuzin
 */
public class MultiValueCallResultConverter<T, R extends List<T>>
        implements ValueConverter<ArrayValue, MultiValueCallResult<T, R>> {

    private static final long serialVersionUID = 20200708L;

    private final ValueConverter<ArrayValue, R> valueConverter;

    /**
     * Basic constructor
     *
     * @param valueConverter converter of multi-return result items to list
     */
    public MultiValueCallResultConverter(ValueConverter<ArrayValue, R> valueConverter) {
        this.valueConverter = valueConverter;
    }

    @Override
    public MultiValueCallResult<T, R> fromValue(ArrayValue value) {
        return new MultiValueCallResultImpl<>(value, valueConverter);
    }
}
