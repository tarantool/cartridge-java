package io.tarantool.driver.mappers;

import io.tarantool.driver.TarantoolClient;
import org.msgpack.value.ArrayValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for TarantoolResultMapper handling result of CRUD operation results returned by Tarantool server
 *
 * @author Alexey Kuzin
 */
public class TarantoolResultMapperFactory {

    private TarantoolClient client;
    private Map<Class<?>, MessagePackValueMapper> mapperCache = new ConcurrentHashMap<>();

    public TarantoolResultMapperFactory(TarantoolClient client) {
        this.client = client;
    }

    /**
     * Create TarantoolResultMapper instance with the passed converter.
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return new TarantoolResultMapper instance
     */
    public <T> MessagePackValueMapper withConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return withConverter(MapperReflectionUtils.getConverterTargetType(valueConverter), valueConverter);
    }

    /**
     * Create TarantoolResultMapper instance with the passed converter.
     * @param objectClass target object type class. Necessary for resolving ambiguity when more than one suitable
     *        converters are present in the configured mapper
     * @param valueConverter entity-to-object converter
     * @param <T> target object type
     * @return new TarantoolResultMapper instance
     */
    public <T> MessagePackValueMapper withConverter(Class<T> objectClass, ValueConverter<ArrayValue, T> valueConverter) {
        MessagePackValueMapper mapper = mapperCache.get(objectClass);
        if (mapper == null) {
            mapper = createMapper(valueConverter);
            mapperCache.put(objectClass, mapper);
        }
        return mapper;
    }

    private <T> MessagePackValueMapper createMapper(ValueConverter<ArrayValue, T> valueConverter) {
        MessagePackValueMapper mapper = client.getConfig().getValueMapper();
        return new TarantoolResultMapper<>(mapper, valueConverter);
    }
}
