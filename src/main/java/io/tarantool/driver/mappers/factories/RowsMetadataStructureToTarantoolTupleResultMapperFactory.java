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
        ValueConverter valueConverter =
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            messagePackMapper.copy(),
            ValueType.ARRAY,
            valueConverter
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackValueMapper valueMapper,
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        ValueConverter valueConverter =
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            valueMapper,
            ValueType.ARRAY,
            valueConverter
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter,
        Class<? extends TarantoolResult<TarantoolTuple>> resultClass) {
        ValueConverter valueConverter =
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            messagePackMapper.copy(),
            ValueType.ARRAY,
            valueConverter,
            resultClass
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackValueMapper valueMapper,
        ArrayValueToTarantoolTupleConverter tupleConverter,
        Class<? extends TarantoolResult<TarantoolTuple>> resultClass) {
        ValueConverter valueConverter =
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            valueMapper,
            ValueType.ARRAY,
            valueConverter,
            resultClass
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withMapValueToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        ValueConverter valueConverter =
            new MapValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            messagePackMapper.copy(),
            ValueType.MAP,
            valueConverter
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withMapValueToTarantoolTupleResultConverter(
        MessagePackValueMapper valueMapper,
        ArrayValueToTarantoolTupleConverter tupleConverter) {
        ValueConverter valueConverter =
            new MapValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            valueMapper,
            ValueType.MAP,
            valueConverter
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withMapValueToTarantoolTupleResultConverter(
        ArrayValueToTarantoolTupleConverter tupleConverter,
        Class<? extends TarantoolResult<TarantoolTuple>> resultClass) {
        ValueConverter valueConverter =
            new MapValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            messagePackMapper.copy(),
            ValueType.MAP,
            valueConverter,
            resultClass
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withMapValueToTarantoolTupleResultConverter(
        MessagePackValueMapper valueMapper,
        ArrayValueToTarantoolTupleConverter tupleConverter,
        Class<? extends TarantoolResult<TarantoolTuple>> resultClass) {
        ValueConverter valueConverter =
            new MapValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverter(
            valueMapper,
            ValueType.MAP,
            valueConverter,
            resultClass
        );
    }

    public TarantoolResultMapper<TarantoolTuple> withTarantoolTupleResultMapper(
        ArrayValueToTarantoolTupleConverter tupleConverter,
        Class<? extends TarantoolResult<TarantoolTuple>> resultClass) {
        // We need cast because we can't use TarantoolResult<TarantoolTuple> in generic
        ValueConverter arrayConverter =
            new ArrayValueToTarantoolTupleResultConverter(tupleConverter);
        ValueConverter mapValueConverter =
            new MapValueToTarantoolTupleResultConverter(tupleConverter);
        return withConverters(
            messagePackMapper.copy(),
            Arrays.asList(
                new ValueConverterWithInputTypeWrapper<>(ValueType.ARRAY, arrayConverter),
                new ValueConverterWithInputTypeWrapper<>(ValueType.MAP, mapValueConverter)
            ),
            resultClass
        );
    }
}
