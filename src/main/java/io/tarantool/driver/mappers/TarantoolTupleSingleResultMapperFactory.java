package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolTupleSingleResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

/**
 * Factory for {@link CallResultMapper} instances used for handling results with {@link TarantoolTuple}s
 *
 * @author Alexey Kuzin
 */
public class TarantoolTupleSingleResultMapperFactory
        extends SingleValueTarantoolResultMapperFactory<TarantoolTuple> {

    public TarantoolTupleSingleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get default {@link TarantoolTuple} converter
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     * @param spaceMetadata configured {@link TarantoolSpaceMetadata} instance
     * @return default mapper instance configured with {@link DefaultTarantoolTupleValueConverter} instance
     */
    public CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withDefaultTupleValueConverter(MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withTarantoolResultConverter(
                new DefaultTarantoolTupleValueConverter(messagePackMapper, spaceMetadata),
                TarantoolTupleSingleResult.class);
    }
}
