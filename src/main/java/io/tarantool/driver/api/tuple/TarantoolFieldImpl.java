package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.springframework.lang.Nullable;

import java.util.Objects;

/**
 * Basic tuple field implementation
 *
 * @author Alexey Kuzin
 */
public class TarantoolFieldImpl implements TarantoolField {

    private Object value;

    /**
     * Deserializing constructor
     * @param value MessagePack value.
     */
    TarantoolFieldImpl(@Nullable Value value) {
        this.value = value;
    }

    /**
     * Serializing constructor
     * @param object entity object
     * @param <O> entity type
     */
    <O> TarantoolFieldImpl(@Nullable O object) {
        this.value = object;
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return getEntity(mapper);
    }

    @SuppressWarnings("unchecked")
    private Value getEntity(MessagePackObjectMapper mapper) {
        if (value == null) {
            return ValueFactory.newNil();
        }
        if (value instanceof Value) {
            return (Value) value;
        }
        return mapper.toValue(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O> O getValue(Class<O> targetClass, MessagePackValueMapper mapper) {
        if (value == null) {
            return null;
        }
        if (value instanceof Value) {
            return mapper.fromValue((Value) value, targetClass);
        }
        if (value.getClass().isAssignableFrom(targetClass)) {
            return (O) value;
        }
        throw new UnsupportedOperationException(
                String.format("Cannot convert field value of type %s to type %s", value.getClass(), targetClass));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TarantoolFieldImpl)) return false;
        TarantoolFieldImpl that = (TarantoolFieldImpl) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
