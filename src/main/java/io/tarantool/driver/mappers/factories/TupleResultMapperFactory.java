package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolResultMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.TarantoolResultConverter;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with tuples of any type
 *
 * @author Alexey Kuzin
 */
public class TupleResultMapperFactory<T> extends TarantoolResultMapperFactory<T> {

    private final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     */
    public TupleResultMapperFactory() {
        this(DefaultMessagePackMapperFactory.getInstance().emptyMapper());
    }

    /**
     * Basic constructor with mapper
     *
     * @param messagePackMapper MessagePack-to-object mapper for tuple contents
     */
    public TupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super();
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param tupleConverter MessagePack-to-entity converter for tuples
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withTupleValueConverter(ValueConverter<ArrayValue, T> tupleConverter) {
        return withConverter(messagePackMapper.copy(), new TarantoolResultConverter<>(tupleConverter));
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param valueMapper    MessagePack-to-object mapper for tuple contents
     * @param tupleConverter MessagePack-to-entity converter for tuples
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withTupleValueConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, T> tupleConverter) {
        return withConverter(valueMapper, new TarantoolResultConverter<>(tupleConverter));
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param tupleConverter MessagePack-to-entity converter for tuples
     * @param resultClass    allows to specify the result type in case if it is impossible to get it via reflection
     *                       (e.g. lambda)
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withTupleValueConverter(
        ValueConverter<ArrayValue, T> tupleConverter,
        Class<? extends TarantoolResult<T>> resultClass) {
        return withConverter(messagePackMapper.copy(), new TarantoolResultConverter<>(tupleConverter), resultClass);
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param valueMapper    MessagePack-to-object mapper for tuple contents
     * @param tupleConverter MessagePack-to-entity converter for tuples
     * @param resultClass    allows to specify the result type in case if it is impossible to get it via reflection
     *                       (e.g. lambda)
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withTupleValueConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<ArrayValue, T> tupleConverter,
        Class<? extends TarantoolResult<T>> resultClass) {
        return withConverter(valueMapper, new TarantoolResultConverter<>(tupleConverter), resultClass);
    }
}
