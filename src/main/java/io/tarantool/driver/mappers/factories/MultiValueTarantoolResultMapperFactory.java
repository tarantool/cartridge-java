package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.TarantoolResultConverter;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call multi-return result items as list of
 * tuples
 *
 * @author Alexey Kuzin
 * @see TarantoolResult
 */
public class MultiValueTarantoolResultMapperFactory<T> extends MultiValueResultMapperFactory<T, TarantoolResult<T>> {

    /**
     * Basic constructor
     */
    public MultiValueTarantoolResultMapperFactory() {
        super();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     */
    public MultiValueTarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueMapper    MessagePack-to-entity mapper for result contents conversion
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withTarantoolResultConverter(MessagePackValueMapper valueMapper, ValueConverter<ArrayValue, T> valueConverter) {
        return withMultiValueResultConverter(valueMapper, new TarantoolResultConverter<>(valueConverter));
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
     * @param valueMapper    MessagePack-to-entity mapper for result contents conversion
     * @param valueConverter the result content converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withTarantoolResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends MultiValueCallResult<T, TarantoolResult<T>>> resultClass) {
        return withMultiValueResultConverter(
            valueMapper, new TarantoolResultConverter<>(valueConverter), resultClass);
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withTarantoolResultConverter(
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends MultiValueCallResult<T, TarantoolResult<T>>> resultClass) {
        return withMultiValueResultConverter(new TarantoolResultConverter<>(valueConverter), resultClass);
    }
}
