package io.tarantool.driver.mappers;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import org.msgpack.value.ArrayValue;

/**
 * Default {@link ArrayValue} to {@link TarantoolTuple} converter
 *
 * @author Sergey Volgin
 */
public class DefaultTarantoolTupleValueConverter implements ValueConverter<ArrayValue, TarantoolTuple> {

    private static final long serialVersionUID = 20200708L;

    private final MessagePackMapper mapper;
    private final TarantoolSpaceMetadata spaceMetadata;

    public DefaultTarantoolTupleValueConverter(MessagePackMapper mapper, TarantoolSpaceMetadata spaceMetadata) {
        this.mapper = mapper;
        this.spaceMetadata = spaceMetadata;
    }

    @Override
    public TarantoolTuple fromValue(ArrayValue value) {
        return new TarantoolTupleImpl(value, mapper, spaceMetadata);
    }
}
