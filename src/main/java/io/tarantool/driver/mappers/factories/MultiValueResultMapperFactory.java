package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.MultiValueCallResultConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call multi-return result which is
 * treated as a list of values
 *
 * @author Alexey Kuzin
 */
public class MultiValueResultMapperFactory<T, R extends List<T>> extends
    TarantoolCallResultMapperFactory<R, MultiValueCallResult<T, R>> {

    private final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     */
    public MultiValueResultMapperFactory() {
        this(DefaultMessagePackMapperFactory.getInstance().emptyMapper());
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     */
    public MultiValueResultMapperFactory(MessagePackMapper messagePackMapper) {
        super();
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Get result mapper for the Lua function call with multi-return result
     *
     * @param valueMapper    MessagePack-to-entity mapper for result contents conversion
     * @param itemsConverter the result list converter
     * @return call result mapper
     */
    public CallResultMapper<R, MultiValueCallResult<T, R>> withMultiValueResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, R> itemsConverter) {
        return withConverter(valueMapper, new MultiValueCallResultConverter<>(itemsConverter));
    }

    /**
     * Get result mapper for the Lua function call with multi-return result
     *
     * @param itemsConverter the result list converter
     * @return call result mapper
     */
    public CallResultMapper<R, MultiValueCallResult<T, R>> withMultiValueResultConverter(
        ValueConverter<ArrayValue, R> itemsConverter) {
        return withConverter(messagePackMapper.copy(), new MultiValueCallResultConverter<>(itemsConverter));
    }

    /**
     * Get result mapper for the Lua function call with multi-return result
     *
     * @param valueMapper    MessagePack-to-entity mapper for result contents conversion
     * @param itemsConverter result list converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<R, MultiValueCallResult<T, R>> withMultiValueResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, R> itemsConverter,
        Class<? extends MultiValueCallResult<T, R>> resultClass) {
        return withConverter(valueMapper, new MultiValueCallResultConverter<>(itemsConverter), resultClass);
    }

    /**
     * Get result mapper for the Lua function call with multi-return result
     *
     * @param itemsConverter result list converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<R, MultiValueCallResult<T, R>> withMultiValueResultConverter(
        ValueConverter<ArrayValue, R> itemsConverter,
        Class<? extends MultiValueCallResult<T, R>> resultClass) {
        return withConverter(
            messagePackMapper.copy(), new MultiValueCallResultConverter<>(itemsConverter), resultClass);
    }
}
