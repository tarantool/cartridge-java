package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;

import java.util.HashMap;
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
        Map<?, ?> result = new HashMap<Object, Object>(value.size(), 1);
        Map<Value, Value> valueMap = value.map();
        for (Value key : valueMap.keySet()) {
            Value val = valueMap.get(key);
            if (!val.isNilValue()) {
                result.put(mapper.fromValue(key), mapper.fromValue(val));
            }
        }
        return result;
    }
}
