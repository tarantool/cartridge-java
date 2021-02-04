package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolTupleResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with {@link TarantoolTuple}s
 *
 * @author Alexey Kuzin
 */
public class TarantoolTupleResultMapperFactory extends TupleResultMapperFactory<TarantoolTuple> {

    public TarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get default {@link TarantoolTuple} converter
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     * @param spaceMetadata configured {@link TarantoolSpaceMetadata} instance
     * @return default mapper instance configured with {@link DefaultTarantoolTupleValueConverter} instance
     */
    public TarantoolResultMapper<TarantoolTuple> withDefaultTupleValueConverter(
            MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withTupleValueConverter(
                new DefaultTarantoolTupleValueConverter(messagePackMapper, spaceMetadata), TarantoolTupleResult.class);
    }
}
