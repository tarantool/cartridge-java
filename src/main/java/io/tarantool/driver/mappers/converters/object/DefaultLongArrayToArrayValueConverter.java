package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableLongValueImpl;

import java.util.Arrays;

/**
 * Default {@code long[]} to {@link ArrayValue} converter
 *
 * @author Anastasiia Romanova
 */
public class DefaultLongArrayToArrayValueConverter implements ObjectConverter<long[], ArrayValue> {

    private static final long serialVersionUID = 20221022L;
    @Override
    public ArrayValue toValue(long[] object) {
        return ValueFactory.newArray(toNumberValueArray(object), true);
    }

    private Value[] toNumberValueArray(long[] object) {
        return Arrays.stream(object).mapToObj(ImmutableLongValueImpl::new).toArray(Value[]::new);
    }
}
