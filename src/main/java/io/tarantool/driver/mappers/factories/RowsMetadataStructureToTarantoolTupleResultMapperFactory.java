package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolResultMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleResultConverter;
import io.tarantool.driver.mappers.converters.value.MapValueToTarantoolTupleResultConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueType;

import java.util.Arrays;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with tuples of any type
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class RowsMetadataStructureToTarantoolTupleResultMapperFactory
    extends RowsMetadataStructureToTarantoolResultMapperFactory<TarantoolTuple> {

    private final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     */
    public RowsMetadataStructureToTarantoolTupleResultMapperFactory() {
        this(DefaultMessagePackMapperFactory.getInstance().emptyMapper());
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-object mapper for tuple contents
     */
    public RowsMetadataStructureToTarantoolTupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super();
        this.messagePackMapper = messagePackMapper;
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return withConverterWithoutTargetClass(
            messagePackMapper.copy(),
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

    public TarantoolResultMapper<TarantoolTuple> withMapValueToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return withConverterWithoutTargetClass(
            messagePackMapper.copy(),
            ValueType.MAP,
            new MapValueToTarantoolTupleResultConverter(tupleConverter)
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withMapValueToTarantoolTupleResultConverter(
        MessagePackValueMapper valueMapper,
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        return withConverterWithoutTargetClass(
            valueMapper,
            ValueType.MAP,
            new MapValueToTarantoolTupleResultConverter(tupleConverter)
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withTarantoolTupleResultMapper(
        ArrayValueToTarantoolTupleConverter tupleConverter,
        Class<? extends TarantoolResult<TarantoolTuple>> resultClass) {
        ValueConverter<ArrayValue, TarantoolResult<TarantoolTuple>> arrayConverter =
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter);
        ValueConverter<MapValue, TarantoolResult<TarantoolTuple>> mapValueConverter =
            new MapValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverterWithoutTargetClass(
            messagePackMapper.copy(),
            Arrays.asList(
                new ValueConverterWithInputTypeWrapper<>(ValueType.ARRAY, arrayConverter),
                new ValueConverterWithInputTypeWrapper<>(ValueType.MAP, mapValueConverter)
            )
        );
    }
}
