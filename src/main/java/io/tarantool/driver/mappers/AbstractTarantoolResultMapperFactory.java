package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for {@link TarantoolResult} mapper factories
 *
 * @author Alexey Kuzin
 */
public abstract class AbstractTarantoolResultMapperFactory {

    protected final MessagePackMapper messagePackMapper;
    protected final Map<Class<?>, MessagePackValueMapper> mapperCache = new ConcurrentHashMap<>();

    /**
     * Basic constructor
     *
     * @param messagePackMapper mapper for MessagePack entities into Java objects and vice versa
     */
    public AbstractTarantoolResultMapperFactory(MessagePackMapper messagePackMapper) {
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Basic constructor with empty mapper
     */
    public AbstractTarantoolResultMapperFactory() {
        this.messagePackMapper = new DefaultMessagePackMapper();
    }

    protected abstract <T> AbstractTarantoolResultMapper<T> createMapper(ValueConverter<ArrayValue, T> valueConverter);

    /**
     * Get default {@link TarantoolTuple} converter
     *
     * @param spaceMetadata configured {@link TarantoolSpaceMetadata} instance
     * @return default mapper instance configured with {@link DefaultTarantoolTupleValueConverter} instance
     */
    public AbstractTarantoolResultMapper<TarantoolTuple> withDefaultTupleValueConverter(
            TarantoolSpaceMetadata spaceMetadata) {
        return withConverter(TarantoolTuple.class,
                new DefaultTarantoolTupleValueConverter(messagePackMapper, spaceMetadata));
    }

    /**
     * Create TarantoolResultMapper instance with the passed converter.
     *
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return a mapper instance
     */
    public <T> AbstractTarantoolResultMapper<T> withConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withConverter(MapperReflectionUtils.getConverterTargetType(valueConverter), valueConverter);
    }

    /**
     * Create TarantoolResultMapper instance with the passed converter.
     *
     * @param tupleClass target object type class. Necessary for resolving ambiguity when more than one suitable
     *        converters are present in the configured mapper
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return a mapper instance
     */
    @SuppressWarnings("unchecked")
    public <T> AbstractTarantoolResultMapper<T> withConverter(Class<T> tupleClass,
                                                      ValueConverter<ArrayValue, T> valueConverter) {
        return (AbstractTarantoolResultMapper<T>) mapperCache.computeIfAbsent(
                tupleClass, c -> createMapper(valueConverter));
    }

}
