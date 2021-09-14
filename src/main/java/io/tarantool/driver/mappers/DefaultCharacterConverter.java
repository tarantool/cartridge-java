package io.tarantool.driver.mappers;

import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link Character} to {@link StringValue} converter
 *
 * @author Ivan Dneprov
 */
public class DefaultCharacterConverter implements
        ValueConverter<StringValue, Character>, ObjectConverter<Character, StringValue> {

    private static final long serialVersionUID = 20210908L;

    @Override
    public StringValue toValue(Character object) {
        String stringFromCharacter = String.valueOf(object);
        return ValueFactory.newString(stringFromCharacter);
    }

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
