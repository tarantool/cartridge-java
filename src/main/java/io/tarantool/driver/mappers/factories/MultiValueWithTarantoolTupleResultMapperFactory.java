package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.MultiValueTarantoolTupleResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;

/**
 * Factory for {@link CallResultMapper} instances used for handling results with {@link TarantoolTuple}s as
 * multi-return result items
 *
 * @author Alexey Kuzin
 */
public class MultiValueWithTarantoolTupleResultMapperFactory
    extends MultiValueWithTarantoolResultMapperFactory<TarantoolTuple> {

    private static TarantoolTupleResultMapperFactory tarantoolTupleResultMapperFactory;

    /**
     * Basic constructor
     */
    public MultiValueWithTarantoolTupleResultMapperFactory() {
        super();
        tarantoolTupleResultMapperFactory = new TarantoolTupleResultMapperFactory();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     */
    public MultiValueWithTarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
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
    public CallResultMapper<TarantoolResult<TarantoolTuple>,
        MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>>>
    withMultiValueTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withMultiValueResultConverter(
            tarantoolTupleResultMapperFactory.withTarantoolTupleMapper(messagePackMapper, spaceMetadata),
            MultiValueTarantoolTupleResult.class
        );
    }
}
