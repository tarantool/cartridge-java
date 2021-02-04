package io.tarantool.driver.mappers;

import io.tarantool.driver.api.CallResult;
import org.msgpack.value.ArrayValue;

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
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public TarantoolCallResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Basic constructor with empty mapper
     */
    public TarantoolCallResultMapperFactory() {
        super();
    }

    @Override
    protected CallResultMapper<T, R> createMapper(
            ValueConverter<ArrayValue, ? extends R> valueConverter,
            Class<? extends R> resultClass) {
        return new CallResultMapper<>(messagePackMapper, valueConverter, resultClass);
    }
}
