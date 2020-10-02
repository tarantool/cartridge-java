package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for TarantoolResultMapper handling result of CRUD operation results returned by Tarantool server
 *
 * @author Alexey Kuzin
 */
public class TarantoolResultMapperFactory {

    private final Map<Class<?>, TarantoolResultMapper<?>> mapperCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, TarantoolSingleResultMapper<?>> singleResultMapperCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, ProxyTarantoolResultMapper<?>> proxyMapperCache = new ConcurrentHashMap<>();

    /**
     * Basic constructor
     */
    public TarantoolResultMapperFactory() {
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<ValueConverter<ArrayValue, T>> getValueConverter(Class<T> clazz) {
        TarantoolResultMapper<T> resultMapper = (TarantoolResultMapper<T>) mapperCache.get(clazz);
        return resultMapper == null ? Optional.empty() : Optional.of(resultMapper.getTupleConverter());
    }

    /**
     * Get default {@link TarantoolTuple} converter
     * @param mapper configured {@link MessagePackMapper} instance
     * @return {@link DefaultTarantoolTupleValueConverter} instance
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

    /**
     * Create TarantoolSingleResultMapper instance with the passed converter.
     *
     * @param valueClass target object type class. Necessary for resolving ambiguity when more than one suitable
     * converters are present in the configured mapper
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return TarantoolSingleResultMapper instance
     */
    @SuppressWarnings("unchecked")
    public <T> TarantoolSingleResultMapper<T> withSingleValueConverter(Class<T> valueClass,
            ValueConverter<ArrayValue, T> valueConverter) {
        TarantoolSingleResultMapper<T> mapper =
                (TarantoolSingleResultMapper<T>) singleResultMapperCache.get(valueClass);
        if (mapper == null) {
            mapper = createSingleValueMapper(valueConverter);
            singleResultMapperCache.put(valueClass, mapper);
        }
        return mapper;
    }

    private <T> TarantoolSingleResultMapper<T> createSingleValueMapper(ValueConverter<ArrayValue, T> valueConverter) {
        MessagePackValueMapper mapper = new DefaultMessagePackMapper();
        return new TarantoolSingleResultMapper<>(mapper, valueConverter);
    }

    /**
     * Create TarantoolResultMapper instance with the passed converter.
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return TarantoolResultMapper instance
     */
    public <T> ProxyTarantoolResultMapper<T> withProxyConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withProxyConverter(valueConverter, MapperReflectionUtils.getConverterTargetType(valueConverter));
    }

    /**
     * Create ProxyTarantoolResultMapper instance with the passed converter.
     *
     * @param valueClass target object type class. Necessary for resolving ambiguity when more than one suitable
     * converters are present in the configured mapper
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return ProxyTarantoolResultMapper instance
     */
    @SuppressWarnings("unchecked")
    public <T> ProxyTarantoolResultMapper<T> withProxyConverter(
            ValueConverter<ArrayValue, T> valueConverter, Class<T> valueClass) {
        ProxyTarantoolResultMapper<T> mapper = (ProxyTarantoolResultMapper<T>) proxyMapperCache.get(valueClass);
        if (mapper == null) {
            mapper = createProxyValueMapper(valueConverter);
            proxyMapperCache.put(valueClass, mapper);
        }
        return mapper;
    }

    private <T> ProxyTarantoolResultMapper<T> createProxyValueMapper(ValueConverter<ArrayValue, T> valueConverter) {
        MessagePackValueMapper mapper = new DefaultMessagePackMapper();
        return new ProxyTarantoolResultMapper<>(mapper, valueConverter);
    }
}
