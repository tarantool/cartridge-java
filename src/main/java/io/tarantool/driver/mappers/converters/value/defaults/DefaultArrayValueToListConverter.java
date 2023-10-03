package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Default {@link ArrayValue} to {@link List} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultArrayValueToListConverter implements ValueConverter<ArrayValue, List<?>> {

    private static final long serialVersionUID = 20220418L;

    private final MessagePackValueMapper mapper;

    public DefaultArrayValueToListConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<?> fromValue(ArrayValue values) {
        ArrayList<Object> objects = new ArrayList<>(values.size());
        for (Value value : values) {
            objects.add(mapper.fromValue(value));
        }
        return objects;
    }
}
