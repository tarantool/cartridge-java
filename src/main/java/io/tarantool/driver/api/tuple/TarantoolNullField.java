package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.NilValue;
import org.msgpack.value.ValueFactory;
import org.springframework.lang.Nullable;

/**
 * Represents a field with empty value which serializes to {@code msgpack.NULL}
 *
 * @author Alexey Kuzin
 */
public class TarantoolNullField<T> implements TarantoolField<T, NilValue> {
    @Nullable
    @Override
    public T getValue() {
        return null;
    }

    @Override
    public NilValue toMessagePackValue(MessagePackObjectMapper mapper) {
        return ValueFactory.newNil();
    }
}
