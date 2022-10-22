package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableLongValueImpl;

import java.util.Arrays;

/**
 * Default {@code long[]} to {@link ArrayValue} converter
 */
public class DefaultLongArrayToArrayValueConverter implements ObjectConverter<long[], ArrayValue> {

    @Override
    public ArrayValue toValue(long[] object) {
        return ValueFactory.newArray(toNumberValue(object), true);
    }

    private Value[] toNumberValue(long[] object) {
        return Arrays.stream(object).mapToObj(ImmutableLongValueImpl::new).toArray(Value[]::new);
    }
}
