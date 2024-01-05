package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default {@link Map} to {@link MapValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultMapToMapValueConverter implements ObjectConverter<Map<?, ?>, MapValue> {

    private static final long serialVersionUID = 20220418L;

    private final MessagePackObjectMapper mapper;

    public DefaultMapToMapValueConverter(MessagePackObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public MapValue toValue(Map<?, ?> object) {
        Value[] values = new Value[object.size() * 2];
        int i = 0;
        for (Object key : object.keySet()) {
            values[i++] = mapper.toValue(key);
            values[i++] = mapper.toValue(object.get(key));
        }
        return ValueFactory.newMap(values, true);
    }
}
