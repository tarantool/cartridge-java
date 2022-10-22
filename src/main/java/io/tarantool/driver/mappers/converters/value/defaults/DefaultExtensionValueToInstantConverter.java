package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ExtensionValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Default {@link ExtensionValue} to {@link java.time.Instant} converter
 *
 * @author Anastasiia Romanova
 * @author Artyom Dubinin
 */
public class DefaultExtensionValueToInstantConverter implements ValueConverter<ExtensionValue, Instant> {

    private static final long serialVersionUID = 20221025L;
    private static final byte DATETIME_TYPE = 0x04;

    private Instant fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return Instant.ofEpochSecond(buffer.getLong()).plusNanos(buffer.getInt());
    }

    @Override
    public Instant fromValue(ExtensionValue value) {
        return fromBytes(value.getData());
    }

    @Override
    public boolean canConvertValue(ExtensionValue value) {
        return value.getType() == DATETIME_TYPE;
    }
}
