package io.tarantool.driver.core;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

/**
 * Singleton Factory implementation to provide instance of TarantoolResultImpl objects.
 *
 * @author Rishal Dev Singh
 */
public class TarantoolResultFactory {
    private static final TarantoolResultFactory INSTANCE = new TarantoolResultFactory();

    public static TarantoolResultFactory getInstance() {
        return INSTANCE;
    }

    public <T> TarantoolResultImpl<T> createTarantoolResultImpl(ArrayValue value,
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
