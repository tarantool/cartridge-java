package io.tarantool.driver.mappers;

import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Default {@link List} to {@link ArrayValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultListValueConverter implements ValueConverter<ArrayValue, List<?>> {

    private MessagePackValueMapper mapper;

    public DefaultListValueConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<?> fromValue(ArrayValue value) {
        return value.list().stream().map(mapper::fromValue).collect(Collectors.toList());
    }
}
