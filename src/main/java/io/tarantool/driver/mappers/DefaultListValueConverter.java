package io.tarantool.driver.mappers;

import org.msgpack.value.ArrayValue;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Default {@link ArrayValue} to {@link List} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultListValueConverter implements ValueConverter<ArrayValue, List<?>> {

    private static final long serialVersionUID = 20200708L;

    private MessagePackValueMapper mapper;

    public DefaultListValueConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<?> fromValue(ArrayValue value) {
        return value.list().stream().map(mapper::fromValue).collect(Collectors.toList());
    }
}
