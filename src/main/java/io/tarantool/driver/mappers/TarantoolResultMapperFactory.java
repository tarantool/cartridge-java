package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;
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
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public TarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Basic constructor with empty mapper
     */
    public TarantoolResultMapperFactory() {
        super();
    }

    @Override
    protected TarantoolResultMapper<T> createMapper(
            ValueConverter<ArrayValue, ? extends TarantoolResult<T>> valueConverter,
            Class<? extends TarantoolResult<T>> resultClass) {
        return new TarantoolResultMapper<>(messagePackMapper, valueConverter, resultClass);
    }
}
