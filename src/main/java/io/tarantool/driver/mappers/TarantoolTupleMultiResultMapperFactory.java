package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleMultiResult;
import io.tarantool.driver.mappers.converters.value.custom.TarantoolTupleConverter;

/**
 * Factory for {@link CallResultMapper} instances used for handling results with {@link TarantoolTuple}s as
 * multi-return result items
 *
 * @author Alexey Kuzin
 */
public class TarantoolTupleMultiResultMapperFactory
        extends MultiValueTarantoolResultMapperFactory<TarantoolTuple> {

    /**
     * Basic constructor
     */
    public TarantoolTupleMultiResultMapperFactory() {
        super();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     */
    public TarantoolTupleMultiResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get default {@link TarantoolTuple} converter
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     * @param spaceMetadata     configured {@link TarantoolSpaceMetadata} instance
     * @return default mapper instance configured with {@link TarantoolTupleConverter} instance
     */
    public CallResultMapper<TarantoolResult<TarantoolTuple>,
            MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>>>
    withDefaultTupleValueConverter(MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withTarantoolResultConverter(
                new TarantoolTupleConverter(messagePackMapper, spaceMetadata),
                TarantoolTupleMultiResult.class);
    }
}
