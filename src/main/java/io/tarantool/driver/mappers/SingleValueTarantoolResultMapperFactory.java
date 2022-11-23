package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.TarantoolResultConverter;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call results resulting in lists of
 * tuples
 *
 * @author Alexey Kuzin
 * @see TarantoolResult
 */
public class SingleValueTarantoolResultMapperFactory<T> extends SingleValueResultMapperFactory<TarantoolResult<T>> {

    /**
     * Basic constructor
     */
    public SingleValueTarantoolResultMapperFactory() {
        super();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public SingleValueTarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    withTarantoolResultConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withSingleValueResultConverter(new TarantoolResultConverter<>(valueConverter));
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    withTarantoolResultConverter(
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends SingleValueCallResult<TarantoolResult<T>>> resultClass) {
        return withSingleValueResultConverter(new TarantoolResultConverter<>(valueConverter), resultClass);
    }
}
