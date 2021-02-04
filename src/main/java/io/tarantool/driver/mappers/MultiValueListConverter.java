package io.tarantool.driver.mappers;

import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link ArrayValue} to {@link List} converter for given item type
 *
 * @author Alexey Kuzin
 */
public class MultiValueListConverter<T> implements ValueConverter<ArrayValue, List<T>> {

    private final ValueConverter<Value, T> valueConverter;

    public MultiValueListConverter(ValueConverter<Value, T> valueConverter) {
        this.valueConverter = valueConverter;
    }

    @Override
    public List<T> fromValue(ArrayValue value) {
        return value.list().stream().map(valueConverter::fromValue).collect(Collectors.toList());
    }
}
