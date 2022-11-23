package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolResultMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolResultConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ValueType;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with tuples of any type
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class RowsMetadataStructureToTarantoolResultMapperFactory<T> extends TarantoolResultMapperFactory<T> {

    private final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     */
    public RowsMetadataStructureToTarantoolResultMapperFactory() {
        this(DefaultMessagePackMapperFactory.getInstance().emptyMapper());
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-object mapper for tuple contents
     */
    public RowsMetadataStructureToTarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        super();
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param valueConverter MessagePack-to-entity converter for tuples
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withArrayValueToTarantoolResultConverter(
        ValueConverter<ArrayValue, T> valueConverter) {
        ValueConverter structureConverter = new ArrayValueToTarantoolResultConverter<>(valueConverter);
        return withConverter(
            messagePackMapper.copy(),
            ValueType.ARRAY,
            structureConverter
        );
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param valueMapper    MessagePack-to-object mapper for tuple contents
     * @param valueConverter MessagePack-to-entity converter for tuples
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withArrayValueToTarantoolResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, T> valueConverter) {
        ValueConverter structureConverter = new ArrayValueToTarantoolResultConverter<>(valueConverter);
        return withConverter(
            valueMapper,
            ValueType.ARRAY,
            structureConverter
        );
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param valueConverter MessagePack-to-entity converter for tuples
     * @param resultClass    allows to specify the result type in case if it is impossible to get it via reflection
     *                       (e.g. lambda)
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withArrayValueToTarantoolResultConverter(
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends TarantoolResult<T>> resultClass) {
        ValueConverter structureConverter = new ArrayValueToTarantoolResultConverter<>(valueConverter);
        return withConverter(
            messagePackMapper.copy(),
            ValueType.ARRAY,
            structureConverter,
            resultClass
        );
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param valueMapper    MessagePack-to-object mapper for tuple contents
     * @param valueConverter MessagePack-to-entity converter for tuples
     * @param resultClass    allows to specify the result type in case if it is impossible to get it via reflection
     *                       (e.g. lambda)
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withArrayValueToTarantoolResultConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, T> valueConverter,
        Class<? extends TarantoolResult<T>> resultClass) {
        ValueConverter structureConverter = new ArrayValueToTarantoolResultConverter<>(valueConverter);
        return withConverter(
            valueMapper,
            ValueType.ARRAY,
            structureConverter,
            resultClass
        );
    }
}
