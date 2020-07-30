package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolValueConverterNotFoundException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
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

    private V entity;

    private DefaultMessagePackMapperFactory mapperFactory;

    TarantoolFieldImpl(V entity, DefaultMessagePackMapperFactory mapperFactory) {
        this.mapperFactory = mapperFactory;
        this.entity = entity;
    }

    <O> TarantoolFieldImpl(@Nullable O value, DefaultMessagePackMapperFactory mapperFactory) {
        this.mapperFactory = mapperFactory;
        this.entity = value == null ? null : this.mapperFactory.defaultComplexTypesMapper().toValue(value);
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return entity == null ? ValueFactory.newNil() : entity;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O> O getValue(Class<O> targetClass) throws TarantoolValueConverterNotFoundException {
        return this.mapperFactory.defaultComplexTypesMapper()
                .getValueConverter((Class<V>) entity.getClass(), targetClass)
                .orElseThrow(() -> new TarantoolValueConverterNotFoundException(entity.getClass(), targetClass))
                .fromValue(entity);
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
