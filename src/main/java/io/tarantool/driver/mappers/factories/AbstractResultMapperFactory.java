package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.AbstractResultMapper;
import io.tarantool.driver.mappers.InterfaceParameterClassNotFoundException;
import io.tarantool.driver.mappers.MapperReflectionUtils;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;

/**
 * Base class for result mapper factories.
 *
 * @param <T> target result mapper type
 * @param <O> target result type
 * @author Alexey Kuzin
 */
public abstract class AbstractResultMapperFactory<O, T extends AbstractResultMapper<? extends O>> {

    /**
     * Basic constructor
     */
    public AbstractResultMapperFactory() {
    }

    /**
     * Instantiate the mapper for result contents
     *
     * @param valueMapper    MessagePack value-to-object mapper for result contents
     * @param valueType      MessagePack source type
     * @param valueConverter converter for result contents (an array)
     * @param resultClass    result type
     * @return new mapper instance
     */
    protected abstract T createMapper(
        MessagePackValueMapper valueMapper,
        ValueType valueType,
        ValueConverter<? extends Value, ? extends O> valueConverter,
        Class<? extends O> resultClass);

    protected abstract T createMapper(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<O>> converters,
        Class<? extends O> resultClass);

    /**
     * Create {@link AbstractResultMapper} instance with the passed converter.
     *
     * @param valueMapper    MessagePack value-to-object mapper for result contents
     * @param valueConverter entity-to-object converter
     * @return a mapper instance
     */
    public T withConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<? extends Value, ? extends O> valueConverter) {
        return withConverter(valueMapper, ValueType.ARRAY, valueConverter);
    }

    public T withConverter(
        MessagePackValueMapper valueMapper, ValueType valueType,
        ValueConverter<? extends Value, ? extends O> valueConverter) {
        try {
            return withConverter(
                valueMapper, valueType, valueConverter, MapperReflectionUtils.getConverterTargetType(valueConverter));
        } catch (InterfaceParameterClassNotFoundException e) {
            throw new TarantoolClientException(e);
        }
    }

    /**
     * Create {@link AbstractResultMapper} instance with the passed converter.
     *
     * @param valueMapper    MessagePack value-to-object mapper for result contents
     * @param valueConverter entity-to-object converter
     * @param resultClass    target result type class. Necessary for resolving ambiguity when more than one suitable
     *                       converters are present in the configured mapper
     * @return a mapper instance
     */
    public T withConverter(
        MessagePackValueMapper valueMapper,
        ValueConverter<? extends Value, ? extends O> valueConverter,
        Class<? extends O> resultClass) {
        return withConverter(valueMapper, ValueType.ARRAY, valueConverter, resultClass);
    }

    public T withConverter(
        MessagePackValueMapper valueMapper,
        ValueType valueType,
        ValueConverter<? extends Value, ? extends O> valueConverter,
        Class<? extends O> resultClass) {
        return createMapper(valueMapper, valueType, valueConverter, resultClass);
    }

    public T withConverters(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<O>> converters) {
        if (converters.size() < 1) {
            throw new TarantoolClientException("Empty converters list");
        }
        try {
            return withConverters(valueMapper, converters,
                MapperReflectionUtils.getConverterTargetType(converters.get(0).getValueConverter()));
        } catch (InterfaceParameterClassNotFoundException e) {
            throw new TarantoolClientException(e);
        }
    }

    public T withConverters(
        MessagePackValueMapper valueMapper,
        List<ValueConverterWithInputTypeWrapper<O>> converters,
        Class<? extends O> resultClass) {
        return createMapper(valueMapper, converters, resultClass);
    }

}
