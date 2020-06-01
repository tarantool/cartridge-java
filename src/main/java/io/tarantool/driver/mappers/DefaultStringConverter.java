package io.tarantool.driver.mappers;

import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code String} to {@link StringValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultStringConverter implements ValueConverter<StringValue, String>, ObjectConverter<String, StringValue> {
    @Override
    public StringValue toValue(String object) {
        return ValueFactory.newString(object);
    }

    @Override
    public String fromValue(StringValue value) {
        return value.asString();
    }
}
