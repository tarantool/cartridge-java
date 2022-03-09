package io.tarantool.driver.mappers.converters.value.custom;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * {@link ArrayValue} to {@link List} converter for given item type
 *
 * @author Alexey Kuzin
 */
public class MultiValueListConverter<T, R extends List<T>, V extends Value> implements ValueConverter<ArrayValue, R> {

    private static final long serialVersionUID = 20200708L;

    private final ValueConverter<V, T> valueConverter;
    private final Supplier<R> containerSupplier;

    /**
     * Basic constructor
     *
     * @param valueConverter converter for result items
     * @param containerSupplier supplier for an empty collection of the result type
     */
    public MultiValueListConverter(ValueConverter<V, T> valueConverter, Supplier<R> containerSupplier) {
        this.valueConverter = valueConverter;
        this.containerSupplier = containerSupplier;
    }

    @Override
    public R fromValue(ArrayValue value) {
        return value.list().stream()
                .map(v -> valueConverter.fromValue((V) v))
                .collect(Collectors.toCollection(containerSupplier));
    }
}
