package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolResultImpl;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * @author Artyom Dubinin
 */
public class ArrayValueToTarantoolTupleResultConverter
    implements ValueConverter<ArrayValue, TarantoolResult<TarantoolTuple>> {

    private static final long serialVersionUID = -1348387430063097175L;

    private final ValueConverter<ArrayValue, TarantoolTuple> tupleConverter;

    public ArrayValueToTarantoolTupleResultConverter(
        ValueConverter<ArrayValue, TarantoolTuple> tupleConverter) {
        super();
        this.tupleConverter = tupleConverter;
    }

    @Override
    public TarantoolResult<TarantoolTuple> fromValue(ArrayValue value) {
        return new TarantoolResultImpl<>(value, tupleConverter);
    }
}