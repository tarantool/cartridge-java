package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.StringValue;

/**
 * Default {@link StringValue} to {@link Character} converter
 *
 * @author Ivan Dneprov
 * @author Artyom Dubinin
 */
public class DefaultStringValueToCharacterConverter implements ValueConverter<StringValue, Character> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public Character fromValue(StringValue value) {
        String stringFromStringValue = value.asString();
        return stringFromStringValue.charAt(0);
    }

    @Override
    public boolean canConvertValue(StringValue value) {
        String stringFromStringValue = value.asString();
        return stringFromStringValue.length() == 1;
    }
}
