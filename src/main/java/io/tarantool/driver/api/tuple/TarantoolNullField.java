package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Objects;

/**
 * Represents a field with empty value which serializes to {@code msgpack.NULL}
 *
 * @author Alexey Kuzin
 */
public final class TarantoolNullField implements TarantoolField {

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return ValueFactory.newNil();
    }

    @Override
    public <O> O getValue(Class<O> targetClass, MessagePackValueMapper mapper) {
        return null;
    }

    @Override
    public Object getValue(MessagePackValueMapper mapper) {
        return null;
    }

    @Override
    public boolean canConvertValue(Class<?> targetClass, MessagePackValueMapper mapper) {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof TarantoolNullField;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
