package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolValueConverterNotFoundException;
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
public class TarantoolFieldImpl<V extends Value> implements TarantoolField {

    private V value;

    private MessagePackMapper mapper;

    /**
     * Deserializing constructor. Takes a MessagePack value and a value mapper.
     * @param value MessagePack value
     * @param mapper value mapper
     */
    TarantoolFieldImpl(V value, MessagePackMapper mapper) {
        this.mapper = mapper;
        this.value = value;
    }

    /**
     * Serializing constructor. Takes an entity object and an object mapper.
     * @param object entity object
     * @param mapper object mapper
     * @param <O> entity type
     */
    <O> TarantoolFieldImpl(@Nullable O object, MessagePackMapper mapper) {
        this.mapper = mapper;
        this.value = object == null ? null : this.mapper.toValue(object);
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return value == null ? ValueFactory.newNil() : value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O> O getValue(Class<O> targetClass) throws TarantoolValueConverterNotFoundException {
        return this.mapper
                .getValueConverter((Class<V>) value.getClass(), targetClass)
                .orElseThrow(() -> new TarantoolValueConverterNotFoundException(value.getClass(), targetClass))
                .fromValue(value);
    }

    @Override
    public byte[] getByteArray() throws TarantoolValueConverterNotFoundException {
        return getValue(byte[].class);
    }

    @Override
    public Boolean getBoolean() throws TarantoolValueConverterNotFoundException {
        return getValue(Boolean.class);
    }

    @Override
    public Double getDouble() throws TarantoolValueConverterNotFoundException {
        return getValue(Double.class);
    }

    @Override
    public Integer getInteger() throws TarantoolValueConverterNotFoundException {
        return getValue(Integer.class);
    }

    @Override
    public String getString() throws TarantoolValueConverterNotFoundException {
        return getValue(String.class);
    }

    @Override
    public UUID getUUID() throws TarantoolValueConverterNotFoundException {
        return getValue(UUID.class);
    }

    @Override
    public BigDecimal getDecimal() throws TarantoolValueConverterNotFoundException {
        return getValue(BigDecimal.class);
    }
}
