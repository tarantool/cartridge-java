package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResultImpl;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Optional;

/**
 * Mapper from array of MessagePack to TarantoolResult with single value
 *
 * @param <T> target type
 * @author Sergey Volgin
 */
public class TarantoolSingleResultMapper<T> implements MessagePackValueMapper {

    private final MessagePackValueMapper valueMapper;
    private final ValueConverter<ArrayValue, T> tupleConverter;

    /**
     * Basic constructor
     *
     * @param valueMapper    value mapper to be used for tuple fields
     * @param tupleConverter MessagePack entity to java object
     */
    public TarantoolSingleResultMapper(MessagePackValueMapper valueMapper,
                                       ValueConverter<ArrayValue, T> tupleConverter) {
        this.valueMapper = valueMapper;
        this.tupleConverter = tupleConverter;

        valueMapper.registerValueConverter(ArrayValue.class, TarantoolResultImpl.class,
                v -> new TarantoolResultImpl<>(ValueFactory.newArray(v), tupleConverter)
        );
    }

    public ValueConverter<ArrayValue, T> getTupleConverter() {
        return tupleConverter;
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
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, Class<O> objectClass,
                                                            ValueConverter<V, O> converter) {
        valueMapper.registerValueConverter(valueClass, objectClass, converter);
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<V> entityClass,
                                                                                 Class<O> objectClass) {
        return valueMapper.getValueConverter(entityClass, objectClass);
    }
}
