package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Default {@link UUID} to {@link ExtensionValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultUUIDToExtensionValueConverter implements ObjectConverter<UUID, ExtensionValue> {

    private static final long serialVersionUID = 20220418L;

    private static final byte UUID_TYPE = 0x02;

    private byte[] toBytes(UUID value) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);

        long mostSignificant = value.getMostSignificantBits();
        buffer.putInt((int) (mostSignificant >>> 32))
            .putShort((short) ((mostSignificant & 0x00000000FFFFFFFFL) >>> 16))
            .putShort((short) (mostSignificant & 0x000000000000FFFFL));

        long leastSignificant = value.getLeastSignificantBits();
        buffer.put((byte) (leastSignificant >>> 56))
            .put((byte) ((leastSignificant & 0x00FF000000000000L) >>> 48))
            .put((byte) ((leastSignificant & 0x0000FF0000000000L) >>> 40))
            .put((byte) ((leastSignificant & 0x000000FF00000000L) >>> 32))
            .put((byte) ((leastSignificant & 0x00000000FF000000L) >>> 24))
            .put((byte) ((leastSignificant & 0x0000000000FF0000L) >>> 16))
            .put((byte) ((leastSignificant & 0x000000000000FF00L) >>> 8))
            .put((byte) (leastSignificant & 0x00000000000000FFL));
        return buffer.array();
    }

    @Override
    public ExtensionValue toValue(UUID object) {
        return ValueFactory.newExtension(UUID_TYPE, toBytes(object));
    }
}
