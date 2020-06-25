package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.util.Nullable;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Represents a field with empty value which serializes to {@code msgpack.NULL}
 *
 * @author Alexey Kuzin
 */
public class TarantoolNullField implements TarantoolField {

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return ValueFactory.newNil();
    }

    @Nullable
    @Override
    public <O> O getValue(Class<O> targetClass) {
        return null;
    }

    @Override
    public byte[] getByteArray() {
        return null;
    }

    @Nullable
    @Override
    public Boolean getBoolean() {
        return null;
    }

    @Nullable
    @Override
    public Double getDouble() {
        return null;
    }

    @Nullable
    @Override
    public Integer getInteger() {
        return null;
    }

    @Nullable
    @Override
    public String getString() {
        return null;
    }

    @Nullable
    @Override
    public UUID getUUID() {
        return null;
    }

    @Nullable
    @Override
    public BigDecimal getDecimal() {
        return null;
    }
}
