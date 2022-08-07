package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;
import java.util.stream.Collectors;

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
    public List<?> fromValue(ArrayValue value) {
        return value.list().stream().map(mapper::fromValue).collect(Collectors.toList());
    }
}
