package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;

/**
 * Default {@link ArrayValue} to {@link TarantoolTuple} converter
 *
 * @author Sergey Volgin
 */
public class DefaultTarantoolTupleConverter implements ValueConverter<ArrayValue, TarantoolTuple> {

    private MessagePackMapper mapper;
    private TarantoolSpaceMetadata spaceMetadata;

    public DefaultTarantoolTupleConverter(MessagePackMapper mapper) {
        this.mapper = mapper;
    }

    public DefaultTarantoolTupleConverter(MessagePackMapper mapper, TarantoolSpaceMetadata spaceMetadata) {
        this.mapper = mapper;
        this.spaceMetadata = spaceMetadata;
    }

    @Override
    public TarantoolTuple fromValue(ArrayValue value) {
        return new TarantoolTupleImpl(value, mapper, spaceMetadata);
    }
}
