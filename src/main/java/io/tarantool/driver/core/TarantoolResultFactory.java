package io.tarantool.driver.core;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

public class TarantoolResultFactory<T> {
    private static final TarantoolResultFactory INSTANCE = new TarantoolResultFactory();

    public static TarantoolResultFactory getInstance() {
        return INSTANCE;
    }

    public TarantoolResultImpl<T> createTarantoolResultImpl(ArrayValue value,
            ValueConverter<ArrayValue, T> valueConverter) {
        return new TarantoolResultImpl<>(value, valueConverter);
    }

    public TarantoolTupleResultImpl createTarantoolTupleResultImpl(Value value,
            ArrayValueToTarantoolTupleConverter tupleConverter) {
        return new TarantoolTupleResultImpl(value, tupleConverter);
    }

    public TarantoolTupleResultImpl createTarantoolTupleResultImpl(ArrayValue rawTuples,
            TarantoolSpaceMetadata metadata,
            ArrayValueToTarantoolTupleConverter tupleConverter) {
        return new TarantoolTupleResultImpl(rawTuples, metadata, tupleConverter);
    }
}
