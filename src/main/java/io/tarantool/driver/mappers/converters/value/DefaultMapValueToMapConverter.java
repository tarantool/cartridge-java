package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.MapValue;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default {@link MapValue} converter to {@link Map} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultMapValueToMapConverter implements ValueConverter<MapValue, Map<?, ?>> {

    private static final long serialVersionUID = 20220418L;

    private final MessagePackValueMapper mapper;

    public DefaultMapValueToMapConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Map<?, ?> fromValue(MapValue value) {
        return value.map().entrySet().stream()
            .filter(e -> !e.getValue().isNilValue())
            .collect(Collectors.toMap(e -> mapper.fromValue(e.getKey()), e -> mapper.fromValue(e.getValue())));
    }
}
