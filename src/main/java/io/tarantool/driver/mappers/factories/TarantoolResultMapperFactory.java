package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolResultMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling box protocol operation results returning
 * {@link TarantoolResult} (array of tuples)
 *
 * @param <T> target tuple type
 * @author Alexey Kuzin
 */
public class TarantoolResultMapperFactory<T> extends
    AbstractResultMapperFactory<TarantoolResult<T>, TarantoolResultMapper<T>> {

    /**
     * Basic constructor
     */
    public TarantoolResultMapperFactory() {
        super();
    }

    @Override
    protected TarantoolResultMapper<T> createMapper(
        MessagePackValueMapper valueMapper,
        ValueType valueType,
        ValueConverter<? extends Value, ? extends TarantoolResult<T>> valueConverter,
        Class<? extends TarantoolResult<T>> resultClass) {
        return new TarantoolResultMapper<>(valueMapper, valueType, valueConverter, resultClass);
    }

    @Override
    protected TarantoolResultMapper<T> createMapper(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<TarantoolResult<T>>> converters,
        Class<? extends TarantoolResult<T>> resultClass) {
        return new TarantoolResultMapper<>(valueMapper, converters, resultClass);
    }
}
