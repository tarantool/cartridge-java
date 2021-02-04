package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call multi-return result items as list of
 * tuples
 *
 * @author Alexey Kuzin
 * @see TarantoolResult
 */
public class MultiValueTarantoolResultMapperFactory<T> extends MultiValueResultMapperFactory<T, TarantoolResult<T>> {

    public MultiValueTarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withTarantoolResultConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withMultiValueResultConverter(new TarantoolResultConverter<>(valueConverter));
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @param resultClass full result type class
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withTarantoolResultConverter(ValueConverter<ArrayValue, T> valueConverter,
                                 Class<? extends MultiValueCallResult<T, TarantoolResult<T>>> resultClass) {
        return withMultiValueResultConverter(new TarantoolResultConverter<>(valueConverter), resultClass);
    }
}
