package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.core.TarantoolResultFactory;
import io.tarantool.driver.core.TarantoolResultImpl;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * @author Artyom Dubinin
 */
public class ArrayValueToTarantoolResultConverter<T>
    implements ValueConverter<ArrayValue, TarantoolResult<T>> {

    private static final long serialVersionUID = -1348387430063097175L;

    private ValueConverter<ArrayValue, T> valueConverter;
    private final TarantoolResultFactory<T> tarantoolResultFactory;

    public ArrayValueToTarantoolResultConverter() {
        this.tarantoolResultFactory = new TarantoolResultFactory<>();
    }

    public ArrayValueToTarantoolResultConverter(ValueConverter<ArrayValue, T> valueConverter) {
        this.valueConverter = valueConverter;
        this.tarantoolResultFactory = new TarantoolResultFactory<>();
    }

    @Override
    public TarantoolResult<T> fromValue(ArrayValue value) {
        return tarantoolResultFactory.<T>createTarantoolResultImpl(value, valueConverter);
    }
}
