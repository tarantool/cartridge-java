package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.SingleValueTarantoolTupleResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;

/**
 * Factory for {@link CallResultMapper} instances used for handling results with {@link TarantoolTuple}s
 *
 * @author Alexey Kuzin
 */
public class SingleValueWithTarantoolTupleResultMapperFactory
    extends SingleValueWithTarantoolResultMapperFactory<TarantoolTuple> {

    private static TarantoolTupleResultMapperFactory tarantoolTupleResultMapperFactory;

    /**
     * Basic constructor
     */
    public SingleValueWithTarantoolTupleResultMapperFactory() {
        super();
        tarantoolTupleResultMapperFactory = new TarantoolTupleResultMapperFactory();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public SingleValueWithTarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
        tarantoolTupleResultMapperFactory = new TarantoolTupleResultMapperFactory(messagePackMapper);
    }

    /**
     * Get default {@link TarantoolTuple} converter
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     * @param spaceMetadata     configured {@link TarantoolSpaceMetadata} instance
     * @return default mapper instance configured with {@link ArrayValueToTarantoolTupleConverter} instance
     */
    public CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withSingleValueTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withSingleValueResultConverter(
            tarantoolTupleResultMapperFactory.
                withTarantoolTupleMapper(messagePackMapper, spaceMetadata),
            SingleValueTarantoolTupleResult.class
        );
    }
}
