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

    private static ArrayValueToTarantoolTupleResultMapperFactory arrayValueToTarantoolTupleResultMapperFactory;
    private static RowsMetadataToTarantoolTupleResultMapperFactory rowsMetadataToTarantoolTupleResultMapperFactory;

    /**
     * Basic constructor
     */
    public SingleValueWithTarantoolTupleResultMapperFactory() {
        super();
        arrayValueToTarantoolTupleResultMapperFactory = new ArrayValueToTarantoolTupleResultMapperFactory();
        rowsMetadataToTarantoolTupleResultMapperFactory = new RowsMetadataToTarantoolTupleResultMapperFactory();
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public SingleValueWithTarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
        arrayValueToTarantoolTupleResultMapperFactory =
            new ArrayValueToTarantoolTupleResultMapperFactory(messagePackMapper);
        rowsMetadataToTarantoolTupleResultMapperFactory = new RowsMetadataToTarantoolTupleResultMapperFactory();
    }

    /**
     * Get default {@link TarantoolTuple} converter
     *
     * @param messagePackMapper MessagePack-to-entity mapper for result contents conversion
     * @param spaceMetadata     configured {@link TarantoolSpaceMetadata} instance
     * @return default mapper instance configured with {@link ArrayValueToTarantoolTupleConverter} instance
     */
    public CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withSingleValueArrayToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withSingleValueResultConverter(
            arrayValueToTarantoolTupleResultMapperFactory.
                withArrayValueToTarantoolTupleResultConverter(messagePackMapper, spaceMetadata),
            SingleValueTarantoolTupleResult.class
        );
    }

    public CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withSingleValueRowsMetadataToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withSingleValueResultConverter(
            rowsMetadataToTarantoolTupleResultMapperFactory.
                withRowsMetadataToTarantoolTupleResultConverter(messagePackMapper, spaceMetadata),
            SingleValueTarantoolTupleResult.class
        );
    }
}
