package io.tarantool.driver.mappers.converters;

import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

/**
 * @author Artyom Dubinin
 */
public class ValueConverterWithInputTypeWrapper<O> {
    private final ValueType valueType;
    private final ValueConverter<? extends Value, ? extends O> valueConverter;

    public ValueConverterWithInputTypeWrapper(
        ValueType valueType, ValueConverter<? extends Value, ? extends O> valueConverter) {
        this.valueType = valueType;
        this.valueConverter = valueConverter;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public ValueConverter<? extends Value, ? extends O> getValueConverter() {
        return valueConverter;
    }
}
