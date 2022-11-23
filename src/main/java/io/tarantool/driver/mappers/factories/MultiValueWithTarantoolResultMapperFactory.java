package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call multi-return result items as list of
 * tuples
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 * @see TarantoolResult
 */
public class MultiValueWithTarantoolResultMapperFactory<T>
    extends MultiValueResultMapperFactory<T, TarantoolResult<T>> {

    private final RowsMetadataStructureToTarantoolResultMapperFactory<T> tarantoolResultMapperFactory;

    /**
     * Basic constructor
     */
    public MultiValueWithTarantoolResultMapperFactory() {
        super();
        tarantoolResultMapperFactory = new RowsMetadataStructureToTarantoolResultMapperFactory<>();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     */
    public MultiValueWithTarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
        tarantoolResultMapperFactory = new RowsMetadataStructureToTarantoolResultMapperFactory<>();
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueMapper    MessagePack-to-entity mapper for result contents conversion
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withMultiValueArrayTarantoolResultConverter(
        MessagePackValueMapper valueMapper, ValueConverter<ArrayValue, T> valueConverter) {
        return withMultiValueResultConverter(valueMapper,
            tarantoolResultMapperFactory.withArrayValueToTarantoolResultConverter(valueConverter));
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withMultiValueArrayTarantoolResultConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withMultiValueResultConverter(
            tarantoolResultMapperFactory.withArrayValueToTarantoolResultConverter(valueConverter));
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
    withMultiValueArrayTarantoolResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends MultiValueCallResult<T, TarantoolResult<T>>> resultClass) {
        return withMultiValueResultConverter(
            valueMapper, tarantoolResultMapperFactory.withArrayValueToTarantoolResultConverter(valueConverter),
            resultClass);
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, MultiValueCallResult<T, TarantoolResult<T>>>
    withMultiValueArrayTarantoolResultConverter(
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends MultiValueCallResult<T, TarantoolResult<T>>> resultClass) {
        return withMultiValueResultConverter(
            tarantoolResultMapperFactory.withArrayValueToTarantoolResultConverter(valueConverter), resultClass);
    }
}
