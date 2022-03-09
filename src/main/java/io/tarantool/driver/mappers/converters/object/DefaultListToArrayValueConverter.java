package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.converters.ObjectConverter;
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
public class DefaultListToArrayValueConverter implements ObjectConverter<List<?>, ArrayValue> {

    private static final long serialVersionUID = 20220418L;

    private final MessagePackObjectMapper mapper;

    public DefaultListToArrayValueConverter(MessagePackObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ArrayValue toValue(List<?> object) {
        Stream<Value> values = object.stream().map(v -> v == null ? ValueFactory.newNil() : mapper.toValue(v));
        return ValueFactory.newArray(values.collect(Collectors.toList()));
    }
}
