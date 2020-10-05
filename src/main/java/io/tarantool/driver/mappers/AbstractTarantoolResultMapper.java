package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResultImpl;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Optional;

/**
 * Base class for TarantoolResult tuple mappers
 *
 * @param <T> target tuple type
 * @author Alexey Kuzin
 */
public abstract class AbstractTarantoolResultMapper<T> implements MessagePackValueMapper {

    protected final MessagePackValueMapper valueMapper;

    public AbstractTarantoolResultMapper(MessagePackValueMapper valueMapper,
                                         ValueConverter<ArrayValue, TarantoolResultImpl> tarantoolResultConverter) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverter(ArrayValue.class, TarantoolResultImpl.class, tarantoolResultConverter);
    }

    @Override
    public <V extends Value, O> O fromValue(V v) throws MessagePackValueMapperException {
        return valueMapper.fromValue(v);
    }

    @Override
    public <V extends Value, O> O fromValue(V v, Class<O> targetClass) throws MessagePackValueMapperException {
        return valueMapper.fromValue(v, targetClass);
    }

    @Override
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass,
                                                            Class<O> objectClass,
                                                            ValueConverter<V, O> converter) {
        valueMapper.registerValueConverter(valueClass, objectClass, converter);
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<V> entityClass,
                                                                                 Class<O> objectClass) {
        return valueMapper.getValueConverter(entityClass, objectClass);
    }
}
