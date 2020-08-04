package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import org.msgpack.value.ArrayValue;

/**
 * Default {@link ArrayValue} to {@link TarantoolTuple} converter
 *
 * @author Sergey Volgin
 */
public class DefaultArrayValueToTarantoolTupleConverter  implements ValueConverter<ArrayValue, TarantoolTuple> {

    private MessagePackMapper mapper;

    public DefaultArrayValueToTarantoolTupleConverter(MessagePackMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public TarantoolTuple fromValue(ArrayValue value) {
        return new TarantoolTupleImpl(value, mapper);
    }
}