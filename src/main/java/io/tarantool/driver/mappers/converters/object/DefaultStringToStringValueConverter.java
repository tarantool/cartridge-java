package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link String} to {@link StringValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultStringToStringValueConverter implements ObjectConverter<String, StringValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public StringValue toValue(String object) {
        return ValueFactory.newString(object);
    }
}
