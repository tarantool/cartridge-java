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

    public TarantoolResultMapper(MessagePackValueMapper valueMapper, ValueConverter<ArrayValue, T> tupleConverter) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverter(ArrayValue.class, tupleConverter);
    }

    @Override
    public <V extends Value, O> O fromValue(V v) throws MessagePackValueMapperException {
        return valueMapper.fromValue(v);
    }

    @Override
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, ValueConverter<V, O> converter) {
        valueMapper.registerValueConverter(valueClass, converter);
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<O> objectClass) {
        return valueMapper.getValueConverter(objectClass);
    }
}
