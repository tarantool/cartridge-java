package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;
import java.util.Optional;

/**
 * Base class for result tuple mappers
 *
 * @param <T> target result type
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public abstract class AbstractResultMapper<T> implements MessagePackValueMapper {

    protected final MessagePackValueMapper valueMapper;

    /**
     * Basic constructor
     *
     * @param valueMapper     MessagePack value-to-object mapper for result contents
     * @param resultConverter converter from MessagePack result array to result type
     * @param resultClass     target result class
     */
    public AbstractResultMapper(
        MessagePackValueMapper valueMapper,
        ValueConverter<? extends Value, ? extends T> resultConverter,
        Class<? extends T> resultClass) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverter(ValueType.ARRAY, resultClass, resultConverter);
    }

    public AbstractResultMapper(
        MessagePackValueMapper valueMapper,
        ValueConverter<? extends Value, ? extends T> resultConverter) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverterWithoutTargetClass(ValueType.ARRAY, resultConverter);
    }

    public AbstractResultMapper(
        MessagePackValueMapper valueMapper,
        ValueType valueType,
        ValueConverter<? extends Value, ? extends T> resultConverter,
        Class<? extends T> resultClass) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverter(valueType, resultClass, resultConverter);
    }

    public AbstractResultMapper(
        MessagePackValueMapper valueMapper,
        ValueType valueType,
        ValueConverter<? extends Value, ? extends T> resultConverter) {
        this.valueMapper = valueMapper;
        valueMapper.registerValueConverterWithoutTargetClass(valueType, resultConverter);
    }

    public AbstractResultMapper(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<T>> converters,
        Class<? extends T> resultClass) {
        this.valueMapper = valueMapper;
        for (ValueConverterWithInputTypeWrapper<T> converter :
            converters) {
            valueMapper.registerValueConverter(
                converter.getValueType(),
                resultClass,
                converter.getValueConverter());
        }
    }

    public AbstractResultMapper(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<T>> converters) {
        this.valueMapper = valueMapper;
        for (ValueConverterWithInputTypeWrapper<T> converter :
            converters) {
            valueMapper.registerValueConverterWithoutTargetClass(
                converter.getValueType(),
                converter.getValueConverter());
        }
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
    public <V extends Value, O> void registerValueConverter(
        ValueType valueType,
        Class<? extends O> objectClass,
        ValueConverter<V, ? extends O> converter) {
        valueMapper.registerValueConverter(valueType, objectClass, converter);
    }

    @Override
    public <V extends Value, O> void registerValueConverterWithoutTargetClass(
        ValueType valueType, ValueConverter<V, ? extends O> converter) {
        valueMapper.registerValueConverterWithoutTargetClass(valueType, converter);
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(
        ValueType valueType,
        Class<O> objectClass) {
        return valueMapper.getValueConverter(valueType, objectClass);
    }
}
