package io.tarantool.driver.mappers;

import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Optional;

/**
 * Base class for result tuple mappers
 *
 * @param <T> target result type
 * @author Alexey Kuzin
 */
public abstract class AbstractResultMapper<T> implements MessagePackValueMapper {

    protected final MessagePackValueMapper valueMapper;

    /**
     * Basic constructor
     *
     * @param valueMapper MessagePack value-to-object mapper for result contents
     * @param resultConverter converter from MessagePack result array to result type
     * @param resultClass target result class
     */
    public AbstractResultMapper(MessagePackValueMapper valueMapper,
                                ValueConverter<ArrayValue, ? extends T> resultConverter,
                                Class<? extends T> resultClass) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverter(ArrayValue.class, resultClass, resultConverter);
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
                                                            Class<? extends O> objectClass,
                                                            ValueConverter<V, ? extends O> converter) {
        valueMapper.registerValueConverter(valueClass, objectClass, converter);
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<V> entityClass,
                                                                                 Class<O> objectClass) {
        return valueMapper.getValueConverter(entityClass, objectClass);
    }
}
