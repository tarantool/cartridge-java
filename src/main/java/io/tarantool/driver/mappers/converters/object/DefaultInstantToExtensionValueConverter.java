package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Default {@link java.time.Instant} to {@link ExtensionValue} converter
 *
 * @author Anastasiia Romanova
 * @author Artyom Dubinin
 */
public class DefaultInstantToExtensionValueConverter implements ObjectConverter<Instant, ExtensionValue> {

    private static final long serialVersionUID = 20221025L;

    private static final byte DATETIME_TYPE = 0x04;

    private byte[] toBytes(Instant value) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value.getEpochSecond());
        buffer.putInt(value.getNano());
        return buffer.array();
    }

    @Override
    public ExtensionValue toValue(Instant object) {
        return ValueFactory.newExtension(DATETIME_TYPE, toBytes(object));
    }
}
