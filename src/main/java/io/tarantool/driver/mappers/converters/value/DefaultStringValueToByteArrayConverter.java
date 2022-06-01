package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.StringValue;

/**
 * Default {@link StringValue} to {@code byte[]} converter
 *
 * @author Artyom Dubinin
 */
public class DefaultStringValueToByteArrayConverter implements ValueConverter<StringValue, byte[]> {

    private static final long serialVersionUID = 20220601L;

    @Override
    public byte[] fromValue(StringValue value) {
        return value.asByteArray();
    }
}
