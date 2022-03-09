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
        Map<Value, Value> values = object.entrySet().stream()
                .collect(Collectors.toMap(e -> mapper.toValue(e.getKey()), e -> mapper.toValue(e.getValue())));
        return ValueFactory.newMap(values);
    }
}
