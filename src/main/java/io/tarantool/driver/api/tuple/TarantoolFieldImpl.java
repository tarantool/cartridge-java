package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.springframework.lang.Nullable;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Basic tuple field implementation
 *
 * @author Alexey Kuzin
 */
public class TarantoolFieldImpl implements TarantoolField {

    private Object value;

    private MessagePackMapper mapper;

    /**
     * Deserializing constructor. Takes a MessagePack value and a MessagePack mapper.
     * @param value MessagePack value.
     * @param mapper value mapper
     */
    TarantoolFieldImpl(Value value, MessagePackMapper mapper) {
        this.mapper = mapper;
        this.value = value;
    }

    /**
     * Serializing constructor. Takes an entity object and an MessagePack mapper.
     * @param object entity object
     * @param mapper object mapper
     * @param <O> entity type
     */
    <O> TarantoolFieldImpl(@Nullable O object, MessagePackMapper mapper) {
        this.mapper = mapper;
        this.value = object;
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return getEntity();
    }

    private Value getEntity() {
        if (value == null) {
            return ValueFactory.newNil();
        }
        if (value instanceof Value) {
            return (Value) value;
        }
        return this.mapper.toValue(value);
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> O getValue(Class<V> entityClass, Class<O> targetClass) {
        return this.mapper.fromValue((V) value, targetClass);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O> O getValue(Class<O> targetClass) {
        if (value == null) {
            return null;
        }
        if (value instanceof Value) {
            return getValue((Class<? extends Value>) value.getClass(), targetClass);
        }
        if (value.getClass().isAssignableFrom(targetClass)) {
            return (O) value;
        }
        throw new UnsupportedOperationException(
                String.format("Cannot convert field value of type %s to type %s", value.getClass(), targetClass));
    }

    @Override
    public byte[] getByteArray() {
        return getValue(byte[].class);
    }

    @Override
    public Boolean getBoolean() {
        return getValue(Boolean.class);
    }

    @Override
    public Double getDouble() {
        return getValue(Double.class);
    }

    @Override
    public Integer getInteger() {
        return getValue(Integer.class);
    }

    @Override
    public String getString() {
        return getValue(String.class);
    }

    @Override
    public UUID getUUID() {
        return getValue(UUID.class);
    }

    @Override
    public BigDecimal getDecimal() {
        return getValue(BigDecimal.class);
    }
}
