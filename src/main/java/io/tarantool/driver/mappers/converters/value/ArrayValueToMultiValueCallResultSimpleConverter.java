package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.core.MultiValueCallResultImpl;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;

/**
 * Converter of the stored function call result into a {@link MultiValueCallResult} with converter inside
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class ArrayValueToMultiValueCallResultSimpleConverter<T, R extends List<T>>
    implements ValueConverter<ArrayValue, MultiValueCallResult<T, R>> {

    private static final long serialVersionUID = -5298829576218240572L;

    private final ValueConverter<ArrayValue, R> valueConverter;

    /**
     * Basic constructor
     *
     * @param valueConverter converter of multi-return result items to list
     */
    public ArrayValueToMultiValueCallResultSimpleConverter(ValueConverter<ArrayValue, R> valueConverter) {
        this.valueConverter = valueConverter;
    }

    @Override
    public MultiValueCallResult<T, R> fromValue(ArrayValue value) {
        return new MultiValueCallResultImpl<>(value, valueConverter);
    }
}
