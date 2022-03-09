package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.BinaryValue;

/**
 * Default {@link BinaryValue} to {@code byte[]} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultBinaryValueToByteArrayConverter implements ValueConverter<BinaryValue, byte[]> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public byte[] fromValue(BinaryValue value) {
        return value.asByteArray();
    }
}
