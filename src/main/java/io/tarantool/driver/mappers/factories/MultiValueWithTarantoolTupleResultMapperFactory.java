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

    private static ArrayValueToTarantoolTupleResultMapperFactory arrayValueToTarantoolTupleResultMapperFactory;
    private static RowsMetadataToTarantoolTupleResultMapperFactory rowsMetadataToTarantoolTupleResultMapperFactory;

    /**
     * Basic constructor
     */
    public MultiValueWithTarantoolTupleResultMapperFactory() {
        super();
        arrayValueToTarantoolTupleResultMapperFactory = new ArrayValueToTarantoolTupleResultMapperFactory();
        rowsMetadataToTarantoolTupleResultMapperFactory = new RowsMetadataToTarantoolTupleResultMapperFactory();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     */
    public MultiValueWithTarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
        arrayValueToTarantoolTupleResultMapperFactory = new ArrayValueToTarantoolTupleResultMapperFactory();
        rowsMetadataToTarantoolTupleResultMapperFactory = new RowsMetadataToTarantoolTupleResultMapperFactory();
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
    withMultiValueArrayToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withMultiValueResultConverter(
            arrayValueToTarantoolTupleResultMapperFactory.withArrayValueToTarantoolTupleResultConverter(
                messagePackMapper, spaceMetadata),
            MultiValueTarantoolTupleResult.class
        );
    }

    public CallResultMapper<TarantoolResult<TarantoolTuple>,
        MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>>>
    withMultiValueRowsMetadataToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withMultiValueResultConverter(
            rowsMetadataToTarantoolTupleResultMapperFactory.withRowsMetadataToTarantoolTupleResultConverter(
                messagePackMapper, spaceMetadata),
            MultiValueTarantoolTupleResult.class
        );
    }
}
