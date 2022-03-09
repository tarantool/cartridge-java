package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link Character} to {@link StringValue} converter
 *
 * @author Ivan Dneprov
 * @author Artyom Dubinin
 */
public class DefaultCharacterToStringValueConverter implements ObjectConverter<Character, StringValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public StringValue toValue(Character object) {
        String stringFromCharacter = String.valueOf(object);
        return ValueFactory.newString(stringFromCharacter);
    }
}
