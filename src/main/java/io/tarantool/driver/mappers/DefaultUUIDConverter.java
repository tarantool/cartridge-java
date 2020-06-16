package io.tarantool.driver.mappers;

import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Default {@link UUID} to {@link ExtensionValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultUUIDConverter implements
        ValueConverter<ExtensionValue, UUID>, ObjectConverter<UUID, ExtensionValue> {

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

    /*
        most significant
        0xFFFFFFFF00000000 time_low
        0x00000000FFFF0000 time_mid
        0x000000000000F000 version
        0x0000000000000FFF time_hi
        least significant
        0xC000000000000000 variant
        0x3FFF000000000000 clock_seq
        0x0000FFFFFFFFFFFF node
     */
    private UUID fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes); //84b56906-aeed-11ea-b3de-0242ac130004
        long mostSignificant =
                (buffer.getInt() & 0xFFFFFFFFL) << 32 |
                (buffer.getShort() & 0xFFFFL) << 16 |
                (buffer.getShort() & 0xFFFFL);
        long leastSignificant =
                (buffer.get() & 0xFFL) << 56 |
                (buffer.get() & 0xFFL) << 48 |
                (buffer.get() & 0xFFL) << 40 |
                (buffer.get() & 0xFFL) << 32 |
                (buffer.get() & 0xFFL) << 24 |
                (buffer.get() & 0xFFL) << 16 |
                (buffer.get() & 0xFFL) << 8 |
                (buffer.get() & 0xFFL);
        return new UUID(mostSignificant, leastSignificant);
    }

    @Override
    public ExtensionValue toValue(UUID object) {
        return ValueFactory.newExtension(UUID_TYPE, toBytes(object));
    }

    @Override
    public UUID fromValue(ExtensionValue value) {
        return fromBytes(value.getData());
    }

    @Override
    public boolean canConvertValue(ExtensionValue value) {
        return value.getType() == UUID_TYPE;
    }
}
