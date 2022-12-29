package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.core.TarantoolResultImpl;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * @author Artyom Dubinin
 */
public class ArrayValueToTarantoolResultConverter<T>
    extends TarantoolResultImpl
    implements ValueConverter<ArrayValue, TarantoolResult<T>> {

    private static final long serialVersionUID = -1348387430063097175L;

    private ValueConverter<ArrayValue, T> valueConverter;

    public ArrayValueToTarantoolResultConverter() {
        super();
    }

    public ArrayValueToTarantoolResultConverter(ValueConverter<ArrayValue, T> valueConverter) {
        this.valueConverter = valueConverter;
    }

    @Override
    public TarantoolResult<T> fromValue(ArrayValue value) {
        return buildTarantoolResultImpl(value, valueConverter);
    }
}
