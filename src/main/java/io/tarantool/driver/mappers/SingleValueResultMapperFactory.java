package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.SingleValueCallResultConverter;
import org.msgpack.value.Value;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call results resulting in two possible
 * values -- result and error
 *
 * @author Alexey Kuzin
 */
public class SingleValueResultMapperFactory<T> extends TarantoolCallResultMapperFactory<T, SingleValueCallResult<T>> {

    private final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     */
    public SingleValueResultMapperFactory() {
        this(DefaultMessagePackMapperFactory.getInstance().emptyMapper());
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public SingleValueResultMapperFactory(MessagePackMapper messagePackMapper) {
        super();
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Get result mapper for the Lua function call with single result
     *
     * @param valueMapper    MessagePack-to-object mapper for result contents
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<T, SingleValueCallResult<T>> withSingleValueResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<Value, T> valueConverter) {
        return withConverter(valueMapper, new SingleValueCallResultConverter<>(valueConverter));
    }

    /**
     * Get result mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<T, SingleValueCallResult<T>> withSingleValueResultConverter(
        ValueConverter<Value, T> valueConverter) {
        return withConverter(messagePackMapper.copy(), new SingleValueCallResultConverter<>(valueConverter));
    }

    /**
     * Get result mapper for the Lua function call with single result
     *
     * @param valueMapper    MessagePack-to-object mapper for result contents
     * @param valueConverter the result content converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<T, SingleValueCallResult<T>> withSingleValueResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<Value, T> valueConverter,
        Class<? extends SingleValueCallResult<T>> resultClass) {
        return withConverter(valueMapper, new SingleValueCallResultConverter<>(valueConverter), resultClass);
    }

    /**
     * Get result mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<T, SingleValueCallResult<T>> withSingleValueResultConverter(
        ValueConverter<Value, T> valueConverter,
        Class<? extends SingleValueCallResult<T>> resultClass) {
        return withConverter(
            messagePackMapper.copy(), new SingleValueCallResultConverter<>(valueConverter), resultClass);
    }
}
