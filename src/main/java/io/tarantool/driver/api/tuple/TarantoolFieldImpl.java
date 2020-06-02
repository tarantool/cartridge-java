package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

/**
 * Basic tuple field implementation
 *
 * @author Alexey Kuzin
 */
public class TarantoolFieldImpl<T> implements TarantoolField<T, Value> {

    private T value;

    TarantoolFieldImpl(Value value, MessagePackObjectMapper mapper) {
        this(mapper.fromValue(value));
    }

    TarantoolFieldImpl(T value) {
        this.value = value;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        if (value == null) {
            return ValueFactory.newNil();
        }
        return mapper.toValue(value);
    }
}
