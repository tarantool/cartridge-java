package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for TarantoolResultMapper handling result of CRUD operation results returned by Tarantool server
 *
 * @author Alexey Kuzin
 */
public class TarantoolResultMapperFactory {

    private Map<Class<?>, TarantoolResultMapper<?>> mapperCache = new ConcurrentHashMap<>();

    /**
     * Basic constructor
     */
    public TarantoolResultMapperFactory() {
    }

    /**
     * Get default {@link TarantoolTuple} converter
     * @param mapper configured {@link MessagePackMapper} instance
     * @return default DefaultTarantoolTupleValueConverter instance
     */
    public ValueConverter<ArrayValue, TarantoolTuple> getDefaultTupleValueConverter(MessagePackMapper mapper) {
        return new DefaultTarantoolTupleValueConverter(mapper);
    }

    /**
     * Get default {@link TarantoolTuple} converter
     * @param mapper configured {@link MessagePackMapper} instance
     * @param spaceMetadata configured {@link TarantoolSpaceMetadata} instance
     * @return default DefaultTarantoolTupleValueConverter instance
     */
    public ValueConverter<ArrayValue, TarantoolTuple> getDefaultTupleValueConverter(
            MessagePackMapper mapper, TarantoolSpaceMetadata spaceMetadata) {
        return new DefaultTarantoolTupleValueConverter(mapper, spaceMetadata);
    }

    /**
     * Create TarantoolResultMapper instance with the passed converter.
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return TarantoolResultMapper instance
     */
    public <T> TarantoolResultMapper<T> withConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withConverter(MapperReflectionUtils.getConverterTargetType(valueConverter), valueConverter);
    }

    /**
     * Create TarantoolResultMapper instance with the passed converter.
     * @param tupleClass target object type class. Necessary for resolving ambiguity when more than one suitable
     *        converters are present in the configured mapper
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return TarantoolResultMapper instance
     */
    @SuppressWarnings("unchecked")
    public <T> TarantoolResultMapper<T> withConverter(Class<T> tupleClass,
                                                      ValueConverter<ArrayValue, T> valueConverter) {
        TarantoolResultMapper<T> mapper = (TarantoolResultMapper<T>) mapperCache.get(tupleClass);
        if (mapper == null) {
            mapper = createMapper(valueConverter);
            mapperCache.put(tupleClass, mapper);
        }
        return mapper;
    }

    private <T> TarantoolResultMapper<T> createMapper(ValueConverter<ArrayValue, T> valueConverter) {
        MessagePackValueMapper mapper = new DefaultMessagePackMapper();
        return new TarantoolResultMapper<>(mapper, valueConverter);
    }
}
