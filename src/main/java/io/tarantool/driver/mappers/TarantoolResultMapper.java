package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;

/**
 * Mapper from array of MessagePack values to {@link TarantoolResult} with specified inner structure
 *
 * @param <T> target tuple type
 * @author Alexey Kuzin
 */
public class TarantoolResultMapper<T> extends AbstractResultMapper<TarantoolResult<T>> {
    /**
     * Basic constructor
     *
     * @param valueMapper    value mapper to be used for result rows
     * @param valueType      MessagePack source type
     * @param valueConverter MessagePack result array to TarantoolResult with inner specified structure
     * @param resultClass    tuple result class
     */
    public TarantoolResultMapper(
        MessagePackValueMapper valueMapper,
        ValueType valueType,
        ValueConverter<? extends Value, ? extends TarantoolResult<T>> valueConverter,
        Class<? extends TarantoolResult<T>> resultClass) {
        super(valueMapper, valueType, valueConverter, resultClass);
    }

    public TarantoolResultMapper(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<TarantoolResult<T>>> valueConverters,
        Class<? extends TarantoolResult<T>> resultClass) {
        super(valueMapper, valueConverters, resultClass);
    }
}
