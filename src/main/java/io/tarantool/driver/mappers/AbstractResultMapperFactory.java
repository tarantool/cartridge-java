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

    protected final MessagePackMapper messagePackMapper;

    /**
     * Basic constructor
     *
     * @param messagePackMapper mapper for MessagePack entities into Java objects and vice versa
     */
    public AbstractResultMapperFactory(MessagePackMapper messagePackMapper) {
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Basic constructor with empty mapper
     */
    public AbstractResultMapperFactory() {
        this.messagePackMapper = new DefaultMessagePackMapper();
    }

    /**
     * Instantiate the mapper for result contents
     *
     * @param valueConverter converter for result contents (an array)
     * @param resultClass result type
     * @return new mapper instance
     */
    protected abstract T createMapper(ValueConverter<ArrayValue, ? extends O> valueConverter,
                                      Class<? extends O> resultClass);

    /**
     * Create {@link AbstractResultMapper} instance with the passed converter.
     *
     * @param valueConverter entity-to-object converter
     * @return a mapper instance
     */
    public T withConverter(ValueConverter<ArrayValue, ? extends O> valueConverter) {
        try {
            return withConverter(MapperReflectionUtils.getConverterTargetType(valueConverter), valueConverter);
        } catch (InterfaceParameterClassNotFoundException e) {
            throw new TarantoolClientException(e);
        }
    }

    /**
     * Create {@link AbstractResultMapper} instance with the passed converter.
     *
     * @param resultClass target result type class. Necessary for resolving ambiguity when more than one suitable
     *        converters are present in the configured mapper
     * @param valueConverter entity-to-object converter
     * @return a mapper instance
     */
    public T withConverter(Class<? extends O> resultClass, ValueConverter<ArrayValue, ? extends O> valueConverter) {
        return createMapper(valueConverter, resultClass);
    }

}
