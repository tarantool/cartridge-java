package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 * Represents a field with empty value which serializes to {@code msgpack.NULL}
 *
 * @author Alexey Kuzin
 */
public final class TarantoolNullField implements TarantoolField {

    private static final TarantoolNullField EMPTY = new TarantoolNullField(null);
    private final TarantoolNullField value;

    private TarantoolNullField(TarantoolNullField value) {
        this.value = value;
    }

    public static TarantoolNullField empty() {
        return EMPTY;
    }

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
        return TarantoolNullField.class.hashCode();
    }
}
