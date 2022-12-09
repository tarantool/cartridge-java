package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolResultMapper;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import io.tarantool.driver.mappers.converters.value.RowsMetadataToTarantoolTupleResultConverter;
import org.msgpack.value.ValueType;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with tuples of any type
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class RowsMetadataToTarantoolTupleResultMapperFactory
    extends TarantoolResultMapperFactory<TarantoolTuple> {

    private final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     */
    public RowsMetadataToTarantoolTupleResultMapperFactory() {
        this(DefaultMessagePackMapperFactory.getInstance().emptyMapper());
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-object mapper for tuple contents
     */
    public RowsMetadataToTarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super();
        this.messagePackMapper = messagePackMapper;
    }

    public TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper) {
        return withRowsMetadataToTarantoolTupleResultConverter(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, null));
    }

    public TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withRowsMetadataToTarantoolTupleResultConverter(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, spaceMetadata));
    }

    public TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return withConverterWithoutTargetClass(
            messagePackMapper.copy(),
            ValueType.MAP,
            new RowsMetadataToTarantoolTupleResultConverter(tupleConverter)
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        MessagePackValueMapper valueMapper,
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return withConverterWithoutTargetClass(
            valueMapper,
            ValueType.MAP,
            new RowsMetadataToTarantoolTupleResultConverter(tupleConverter)
        );
    }
}
