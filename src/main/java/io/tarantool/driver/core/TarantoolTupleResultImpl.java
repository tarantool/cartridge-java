package io.tarantool.driver.core;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleResult;
import io.tarantool.driver.exceptions.TarantoolTupleConversionException;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.stream.Collectors;

/**
 * Specific TarantoolResult implementation especially for TarantoolTuple input
 *
 * @author Artyom Dubinin
 */
public class TarantoolTupleResultImpl extends TarantoolResultImpl<TarantoolTuple> implements TarantoolTupleResult {

    protected TarantoolTupleResultImpl(
        ArrayValue rawTuples, TarantoolSpaceMetadata metadata,
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        setItems(rawTuples, metadata, tupleConverter);
    }

    protected TarantoolTupleResultImpl(Value value, ArrayValueToTarantoolTupleConverter tupleConverter) {
        setItems(value.asArrayValue(), tupleConverter);
    }

    private void setItems(
        ArrayValue tupleArray, TarantoolSpaceMetadata responseMetadata,
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        this.tuples = tupleArray.list().stream()
            .map(v -> {
                try {
                    return tupleConverter.fromValue(v.asArrayValue(), responseMetadata);
                } catch (MessageTypeCastException e) {
                    throw new TarantoolTupleConversionException(v, e);
                }
            })
            .collect(Collectors.toList());
    }
}
