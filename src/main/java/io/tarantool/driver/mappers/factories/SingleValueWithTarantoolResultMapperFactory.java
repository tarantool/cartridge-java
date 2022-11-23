package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call results resulting in lists of
 * tuples
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 * @see TarantoolResult
 */
public class SingleValueWithTarantoolResultMapperFactory<T> extends SingleValueResultMapperFactory<TarantoolResult<T>> {

    private final RowsMetadataStructureToTarantoolResultMapperFactory<T> tarantoolResultMapperFactory;

    /**
     * Basic constructor
     */
    public SingleValueWithTarantoolResultMapperFactory() {
        super();
        tarantoolResultMapperFactory = new RowsMetadataStructureToTarantoolResultMapperFactory<>();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public SingleValueWithTarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
        tarantoolResultMapperFactory = new RowsMetadataStructureToTarantoolResultMapperFactory<>();
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    withSingleValueArrayTarantoolResultConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withSingleValueResultConverter(
            tarantoolResultMapperFactory.withArrayValueToTarantoolResultConverter(valueConverter));
    }

    /**
     * Get {@link TarantoolResult} mapper for the Lua function call with single result
     *
     * @param valueConverter the result content converter
     * @param resultClass    full result type class
     * @return call result mapper
     */
    public CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    withSingleValueArrayTarantoolResultConverter(
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends SingleValueCallResult<TarantoolResult<T>>> resultClass) {
        return withSingleValueResultConverter(
            tarantoolResultMapperFactory.withArrayValueToTarantoolResultConverter(valueConverter),
            resultClass);
    }
}
