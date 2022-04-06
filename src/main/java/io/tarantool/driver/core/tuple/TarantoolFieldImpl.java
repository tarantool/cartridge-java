package io.tarantool.driver.core.tuple;

import io.tarantool.driver.api.tuple.TarantoolField;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * Basic tuple field implementation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class TarantoolFieldImpl implements TarantoolField {

    private final Object value;

    private Optional<Object> convertedValueWithoutTargetType = Optional.empty();

    private Optional<Class<?>> lastConvertedTargetClass = Optional.empty();
    private Object lastConvertedValue;

    /**
     * Deserializing constructor
     * @param value MessagePack value.
     */
    TarantoolFieldImpl(Value value) {
        this.value = value;
    }

    /**
     * Serializing constructor
     * @param object entity object
     * @param <O> entity type
     */
    <O> TarantoolFieldImpl(O object) {
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
        if (lastConvertedTargetClass.isPresent() && lastConvertedTargetClass.get().equals(targetClass)) {
            return (O) lastConvertedValue;
        }
        if (value instanceof Value) {
            O convertedValue = mapper.fromValue((Value) value, targetClass);
            lastConvertedTargetClass = Optional.of(targetClass);
            lastConvertedValue = convertedValue;
            return convertedValue;
        }
        if (value.getClass().isAssignableFrom(targetClass)) {
            return (O) value;
        }
        throw new UnsupportedOperationException(
                String.format("Cannot convert field value of type %s to type %s", value.getClass(), targetClass));
    }

    @Override
    public Object getValue(MessagePackValueMapper mapper) {
        if (convertedValueWithoutTargetType.isPresent()) {
            return convertedValueWithoutTargetType.get();
        }
        if (value instanceof Value) {
            Object convertedValue = mapper.fromValue((Value) value);
            convertedValueWithoutTargetType = Optional.of(convertedValue);
            return convertedValue;
        }
        return value;
    }

    @Override
    public boolean canConvertValue(Class<?> targetClass, MessagePackValueMapper mapper) {
        return canConvertValueAndCacheResult(((Value) value).getClass(), targetClass, mapper);
    }

    public <V extends Value> boolean
    canConvertValueAndCacheResult(Class<V> entityClass, Class<?> targetClass, MessagePackValueMapper mapper) {
        if (lastConvertedTargetClass.isPresent() && lastConvertedTargetClass.get().equals(targetClass)) {
            return true;
        }
        if (value instanceof Value) {
            Optional<? extends ValueConverter<V, ?>> converter = mapper.getValueConverter(entityClass, targetClass);
            if (converter.isPresent()) {
                Object convertedValue = converter.get().fromValue((V) value);
                lastConvertedTargetClass = Optional.of(targetClass);
                lastConvertedValue = convertedValue;
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TarantoolFieldImpl)) {
            return false;
        }
        TarantoolFieldImpl that = (TarantoolFieldImpl) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
