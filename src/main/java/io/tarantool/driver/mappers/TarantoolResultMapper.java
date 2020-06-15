package io.tarantool.driver.mappers;

import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Optional;

/**
 * Mapper from array of MessagePack tuples to TarantoolResult
 * @param <T> tuple target type
 *
 * @author Alexey Kuzin
 */
public class TarantoolResultMapper<T> implements MessagePackValueMapper {

    private MessagePackValueMapper valueMapper;

    /**
     * Basic constructor
     * @param valueMapper value mapper to be used for tuple fields
     * @param tupleClass target tuple class
     * @param tupleConverter MessagePack entity to tuple converter
     */
    public TarantoolResultMapper(MessagePackValueMapper valueMapper, Class<T> tupleClass, ValueConverter<ArrayValue, T> tupleConverter) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverter(ArrayValue.class, tupleClass, tupleConverter);
    }

    @Override
    public <V extends Value, O> O fromValue(V v) throws MessagePackValueMapperException {
        return valueMapper.fromValue(v);
    }

    @Override
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, Class<O> objectClass, ValueConverter<V, O> converter) {
        valueMapper.registerValueConverter(valueClass, objectClass, converter);
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<V> entityClass, Class<O> objectClass) {
        return valueMapper.getValueConverter(entityClass, objectClass);
    }
}
