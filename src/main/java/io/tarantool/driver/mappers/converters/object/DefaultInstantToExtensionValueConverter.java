package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

import static io.tarantool.driver.mappers.converters.value.defaults.DefaultExtensionValueToInstantConverter.DATETIME_TYPE;

/**
 * Default {@link java.time.Instant} to {@link ExtensionValue} converter
 *
 * @author Anastasiia Romanova
 * @author Artyom Dubinin
 */
public class DefaultInstantToExtensionValueConverter implements ObjectConverter<Instant, ExtensionValue> {

    private static final long serialVersionUID = 20221025L;

    private byte[] toBytes(Instant value) {
        long seconds = value.getEpochSecond();
        Integer nano = value.getNano();

        int size = 8;
        boolean withOptionalFields = !nano.equals(0);
        if (withOptionalFields) {
            size = 16;
        }

        ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(seconds);
        if (withOptionalFields) {
            buffer.putInt(nano);
        }
        return buffer.array();
    }

    @Override
    public ExtensionValue toValue(Instant object) {
        return ValueFactory.newExtension(DATETIME_TYPE, toBytes(object));
    }
}
