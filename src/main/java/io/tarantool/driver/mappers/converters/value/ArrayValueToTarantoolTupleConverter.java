package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Default {@link ArrayValue} to {@link TarantoolTuple} converter
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public class ArrayValueToTarantoolTupleConverter implements ValueConverter<ArrayValue, TarantoolTuple> {

    private static final long serialVersionUID = 20220418L;

    private final MessagePackMapper mapper;
    private final TarantoolSpaceMetadata spaceMetadata;

    public ArrayValueToTarantoolTupleConverter(MessagePackMapper mapper, TarantoolSpaceMetadata spaceMetadata) {
        this.mapper = mapper;
        this.spaceMetadata = spaceMetadata;
    }

    @Override
    public TarantoolTuple fromValue(ArrayValue value) {
        return new TarantoolTupleImpl(value, mapper, spaceMetadata);
    }

    public TarantoolTuple fromValue(ArrayValue value, TarantoolSpaceMetadata responseMetadata) {
        return new TarantoolTupleImpl(value, mapper, responseMetadata);
    }
}
