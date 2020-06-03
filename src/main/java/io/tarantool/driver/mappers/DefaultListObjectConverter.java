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
public class DefaultListObjectConverter implements ObjectConverter<List<?>, ArrayValue> {

    private MessagePackObjectMapper mapper;

    public DefaultListObjectConverter(MessagePackObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ArrayValue toValue(List<?> object) {
        Stream<Value> values = object.stream().map(mapper::toValue);
        return ValueFactory.newArray(values.collect(Collectors.toList()));
    }
}
