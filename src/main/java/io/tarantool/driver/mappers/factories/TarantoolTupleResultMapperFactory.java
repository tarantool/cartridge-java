package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleResult;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolResultMapper;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with {@link TarantoolTuple}s
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class TarantoolTupleResultMapperFactory
    extends RowsMetadataStructureToTarantoolTupleResultMapperFactory {

    /**
     * Basic constructor
     */
    public TarantoolTupleResultMapperFactory() {
        super();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-object mapper for tuple contents
     */
    public TarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get default {@link TarantoolTuple} converter
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     * @param spaceMetadata     configured {@link TarantoolSpaceMetadata} instance
     * @return default mapper instance configured with {@link ArrayValueToTarantoolTupleConverter} instance
     */
    public TarantoolResultMapper<TarantoolTuple> withFlattenTupleMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withArrayValueToTarantoolTupleResultConverter(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, spaceMetadata), TarantoolTupleResult.class);
    }

    public TarantoolResultMapper<TarantoolTuple> withUnflattenTupleMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withMapValueToTarantoolTupleResultConverter(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, spaceMetadata), TarantoolTupleResult.class);
    }

    public MessagePackValueMapper withTarantoolTupleMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withTarantoolTupleResultMapper(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, spaceMetadata), TarantoolTupleResult.class);
    }
}
