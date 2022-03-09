package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

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
            ValueConverter<ArrayValue, ? extends TarantoolResult<T>> valueConverter,
            Class<? extends TarantoolResult<T>> resultClass) {
        return new TarantoolResultMapper<>(valueMapper, valueConverter, resultClass);
    }
}
