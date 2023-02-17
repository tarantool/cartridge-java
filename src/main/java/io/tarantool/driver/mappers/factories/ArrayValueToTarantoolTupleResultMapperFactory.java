package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolResultMapper;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleResultConverter;
import org.msgpack.value.ValueType;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with tuples of array type
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class ArrayValueToTarantoolTupleResultMapperFactory
    extends ArrayValueToTarantoolResultMapperFactory<TarantoolTuple> {

    private final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     */
    public ArrayValueToTarantoolTupleResultMapperFactory() {
        this(DefaultMessagePackMapperFactory.getInstance().emptyMapper());
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-object mapper for tuple contents
     */
    public ArrayValueToTarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super();
        this.messagePackMapper = messagePackMapper;
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper) {
        return withArrayValueToTarantoolTupleResultConverter(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, null));
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return withArrayValueToTarantoolTupleResultConverter(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, spaceMetadata));
    }

    public ValueConverterWithInputTypeWrapper<Object> getArrayValueToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata) {
        return getArrayValueToTarantoolTupleResultConverter(
            new ArrayValueToTarantoolTupleConverter(messagePackMapper, spaceMetadata));
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return withConverterWithoutTargetClass(
            messagePackMapper.copy(),
            ValueType.ARRAY,
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter)
        );
    }

    public ValueConverterWithInputTypeWrapper<Object> getArrayValueToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return new ValueConverterWithInputTypeWrapper<>(
            ValueType.ARRAY,
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter)
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackValueMapper valueMapper,
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return withConverterWithoutTargetClass(
            valueMapper,
            ValueType.ARRAY,
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter)
        );
    }
}
