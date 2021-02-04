package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;
import org.msgpack.value.Value;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call results resulting in two possible
 * values -- result and error
 *
 * @author Alexey Kuzin
 */
public class SingleValueResultMapperFactory<T> extends TarantoolCallResultMapperFactory<T, SingleValueCallResult<T>> {

    public SingleValueResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get result mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<T, SingleValueCallResult<T>> withSingleValueResultConverter(
            ValueConverter<Value, T> valueConverter) {
        return withConverter(new SingleValueCallResultConverter<>(valueConverter));
    }

    /**
     * Get result mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @param resultClass full result type class
     * @return call result mapper
     */
    public CallResultMapper<T, SingleValueCallResult<T>> withSingleValueResultConverter(
            ValueConverter<Value, T> valueConverter,
            Class<? extends SingleValueCallResult<T>> resultClass) {
        return withConverter(resultClass, new SingleValueCallResultConverter<>(valueConverter));
    }
}
