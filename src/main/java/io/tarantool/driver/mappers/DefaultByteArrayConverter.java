package io.tarantool.driver.mappers;

import org.msgpack.value.BinaryValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code byte[]} to {@link BinaryValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultByteArrayConverter implements
        ValueConverter<BinaryValue, byte[]>, ObjectConverter<byte[], BinaryValue> {
    @Override
    public BinaryValue toValue(byte[] object) {
        return ValueFactory.newBinary(object);
    }

    @Override
    public byte[] fromValue(BinaryValue value) {
        return value.asByteArray();
    }
}
