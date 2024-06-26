package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.StringValue;

/**
 * Default {@link StringValue} to {@link String} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultStringValueToStringConverter implements ValueConverter<StringValue, String> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public String fromValue(StringValue value) {
        return value.toString();
    }
}
