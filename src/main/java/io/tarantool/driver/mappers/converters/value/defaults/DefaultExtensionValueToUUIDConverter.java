package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ExtensionValue;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Default {@link ExtensionValue} to {@link UUID} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultExtensionValueToUUIDConverter implements ValueConverter<ExtensionValue, UUID> {

    private static final long serialVersionUID = 20220418L;

    private static final byte UUID_TYPE = 0x02;

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
    public UUID fromValue(ExtensionValue value) {
        return fromBytes(value.getData());
    }

    @Override
    public boolean canConvertValue(ExtensionValue value) {
        return value.getType() == UUID_TYPE;
    }
}
