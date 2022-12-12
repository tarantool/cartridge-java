package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.factories.ArrayValueToTarantoolTupleResultMapperFactory;
import io.tarantool.driver.mappers.factories.MultiValueWithTarantoolTupleResultMapperFactory;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactoryImpl;
import io.tarantool.driver.mappers.factories.RowsMetadataToTarantoolTupleResultMapperFactory;
import io.tarantool.driver.mappers.factories.SingleValueWithTarantoolTupleResultMapperFactory;

/**
 * @author Artyom Dubinin
 */
public final class TarantoolTupleResultMapperFactoryImpl implements TarantoolTupleResultMapperFactory {

    private static final TarantoolTupleResultMapperFactoryImpl instance = new TarantoolTupleResultMapperFactoryImpl();

    private final ArrayValueToTarantoolTupleResultMapperFactory arrayTupleResultMapperFactory;
    private final RowsMetadataToTarantoolTupleResultMapperFactory rowsMetadataToTarantoolTupleResultMapperFactory;
    private final SingleValueWithTarantoolTupleResultMapperFactory singleValueWithTarantoolTupleResultMapperFactory;
    private final MultiValueWithTarantoolTupleResultMapperFactory multiValueWithTarantoolTupleResultMapperFactory;

    private TarantoolTupleResultMapperFactoryImpl() {
        ResultMapperFactoryFactoryImpl
            mapperFactoryFactory = new ResultMapperFactoryFactoryImpl();
        this.arrayTupleResultMapperFactory =
            mapperFactoryFactory.arrayTupleResultMapperFactory();
        this.rowsMetadataToTarantoolTupleResultMapperFactory =
            mapperFactoryFactory.rowsMetadataTupleResultMapperFactory();
        this.singleValueWithTarantoolTupleResultMapperFactory =
            mapperFactoryFactory.singleValueTupleResultMapperFactory();
        this.multiValueWithTarantoolTupleResultMapperFactory =
            mapperFactoryFactory.multiValueTupleResultMapperFactory();
    }

    @Override
    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper) {
        return arrayTupleResultMapperFactory
            .withArrayValueToTarantoolTupleResultConverter(messagePackMapper);
    }

    @Override
    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return arrayTupleResultMapperFactory
            .withArrayValueToTarantoolTupleResultConverter(messagePackMapper, spaceMetadata);
    }

    @Override
    public TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper) {
        return rowsMetadataToTarantoolTupleResultMapperFactory
            .withRowsMetadataToTarantoolTupleResultConverter(messagePackMapper);
    }

    @Override
    public TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return rowsMetadataToTarantoolTupleResultMapperFactory
            .withRowsMetadataToTarantoolTupleResultConverter(messagePackMapper, spaceMetadata);
    }

    @Override
    public CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withSingleValueArrayToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return singleValueWithTarantoolTupleResultMapperFactory
            .withSingleValueArrayToTarantoolTupleResultMapper(messagePackMapper, spaceMetadata);
    }

    @Override
    public CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withSingleValueRowsMetadataToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return singleValueWithTarantoolTupleResultMapperFactory
            .withSingleValueRowsMetadataToTarantoolTupleResultMapper(messagePackMapper, spaceMetadata);
    }

    @Override
    public CallResultMapper<
        TarantoolResult<TarantoolTuple>,
        MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>>>
    withMultiValueArrayToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return multiValueWithTarantoolTupleResultMapperFactory
            .withMultiValueArrayToTarantoolTupleResultMapper(messagePackMapper, spaceMetadata);
    }

    /**
     * Get factory instance.
     *
     * @return factory instance
     */
    public static TarantoolTupleResultMapperFactory getInstance() {
        return instance;
    }
}
