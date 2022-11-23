package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.CallResult;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;

/**
 * Factory for {@link CallResultMapper} instances used for calling API functions on Tarantool instance
 *
 * @param <T> target result content type
 * @param <R> target result type
 * @author Alexey Kuzin
 */
public class TarantoolCallResultMapperFactory<T, R extends CallResult<T>> extends
    AbstractResultMapperFactory<R, CallResultMapper<T, R>> {

    /**
     * Basic constructor
     */
    public TarantoolCallResultMapperFactory() {
        super();
    }

    @Override
    protected CallResultMapper<T, R> createMapper(
        MessagePackValueMapper valueMapper, ValueType valueType,
        ValueConverter<? extends Value, ? extends R> valueConverter,
        Class<? extends R> resultClass) {
        return new CallResultMapper<>(valueMapper, valueConverter, resultClass);
    }

    @Override
    protected CallResultMapper<T, R> createMapper(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<R>> converters, Class<? extends R> resultClass) {
        return new CallResultMapper<>(valueMapper, converters, resultClass);
    }
}
