package io.tarantool.driver.mappers;

import org.msgpack.value.MapValue;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Default {@link MapValue} converter to {@link Map} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultMapValueConverter implements ValueConverter<MapValue, Map<?, ?>> {

    private MessagePackValueMapper mapper;

    public DefaultMapValueConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Map<?, ?> fromValue(MapValue value) {
        return value.map().entrySet().stream()
                .filter(e -> !e.getValue().isNilValue())
                .collect(Collectors.toMap(e -> mapper.fromValue(e.getKey()), e -> mapper.fromValue(e.getValue())));
    }
}
