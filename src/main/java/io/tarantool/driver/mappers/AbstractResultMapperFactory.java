package io.tarantool.driver.mappers;

import io.tarantool.driver.exceptions.TarantoolClientException;
import org.msgpack.value.ArrayValue;

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
     * @param valueMapper MessagePack value-to-object mapper for result contents
     * @param valueConverter converter for result contents (an array)
     * @param resultClass result type
     * @return new mapper instance
     */
    protected abstract T createMapper(MessagePackValueMapper valueMapper,
                                      ValueConverter<ArrayValue, ? extends O> valueConverter,
                                      Class<? extends O> resultClass);

    /**
     * Create {@link AbstractResultMapper} instance with the passed converter.
     *
     * @param valueMapper MessagePack value-to-object mapper for result contents
     * @param valueConverter entity-to-object converter
     * @return a mapper instance
     */
    public T withConverter(MessagePackValueMapper valueMapper, ValueConverter<ArrayValue, ? extends O> valueConverter) {
        try {
            return withConverter(
                    valueMapper, valueConverter, MapperReflectionUtils.getConverterTargetType(valueConverter));
        } catch (InterfaceParameterClassNotFoundException e) {
            throw new TarantoolClientException(e);
        }
    }

    /**
     * Create {@link AbstractResultMapper} instance with the passed converter.
     *
     * @param valueMapper MessagePack value-to-object mapper for result contents
     * @param valueConverter entity-to-object converter
     * @param resultClass target result type class. Necessary for resolving ambiguity when more than one suitable
     *        converters are present in the configured mapper
     * @return a mapper instance
     */
    public T withConverter(MessagePackValueMapper valueMapper,
                           ValueConverter<ArrayValue, ? extends O> valueConverter,
                           Class<? extends O> resultClass) {
        return createMapper(valueMapper, valueConverter, resultClass);
    }

}
