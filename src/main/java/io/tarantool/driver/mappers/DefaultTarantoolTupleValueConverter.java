package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import org.msgpack.value.ArrayValue;

/**
 * Default {@link ArrayValue} to {@link TarantoolTuple} converter
 *
 * @author Sergey Volgin
 */
public class DefaultTarantoolTupleValueConverter implements ValueConverter<ArrayValue, TarantoolTuple> {

    private MessagePackMapper mapper;

    public DefaultTarantoolTupleValueConverter(MessagePackMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public TarantoolTuple fromValue(ArrayValue value) {
        return new TarantoolTupleImpl(value, mapper);
    }
}