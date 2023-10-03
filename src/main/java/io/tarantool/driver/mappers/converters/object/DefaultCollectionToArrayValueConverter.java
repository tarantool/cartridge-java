package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Collection;

/**
 * Default {@link Collection} to {@link ArrayValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultCollectionToArrayValueConverter implements ObjectConverter<Collection<?>, ArrayValue> {

    private static final long serialVersionUID = 20231003L;

    private final MessagePackObjectMapper mapper;

    public DefaultCollectionToArrayValueConverter(MessagePackObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ArrayValue toValue(Collection<?> object) {
        Value[] values = new Value[object.size()];
        int i = 0;
        for (Object value : object) {
            values[i++] = value == null ? ValueFactory.newNil() : mapper.toValue(value);
        }
        return ValueFactory.newArray(values, false);
    }
}
