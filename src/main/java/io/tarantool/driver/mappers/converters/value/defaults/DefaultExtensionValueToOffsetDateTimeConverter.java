package io.tarantool.driver.mappers.converters.value.defaults;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ExtensionValue;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static io.tarantool.driver.mappers.converters.value.defaults.DefaultExtensionValueToInstantConverter.DATETIME_TYPE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.time.ZoneOffset.UTC;

/**
 * Default {@link ExtensionValue} to {@link java.time.OffsetDateTime} converter.
 *
 * @author Valeriy Vyrva
 */
public class DefaultExtensionValueToOffsetDateTimeConverter implements ValueConverter<ExtensionValue, OffsetDateTime> {

    private static final long serialVersionUID = 20231027114017L;

    public static final int SECONDS_PER_MINUTE = 60;

    @Override
    public boolean canConvertValue(ExtensionValue value) {
        return value.getType() == DATETIME_TYPE;
    }

    @Override
    public OffsetDateTime fromValue(ExtensionValue value) {
        return fromBytes(value.getData());
    }

    /**
     * Decode protocol level representation into java object.
     *
     * @param value Bytes from protocol level
     * @return Decoded value
     * @see
     * <a href="https://github.com/tarantool/tarantool/blob/master/src/lib/core/mp_datetime.c#L18">
     * serialization schema</a>
     * @see
     * <a href="https://github.com/tarantool/tarantool/blob/master/src/lib/core/datetime.h#L85">struct datetime</a>
     * @see
     * <a href="https://github.com/tarantool/tarantool/blob/master/src/lib/core/mp_datetime.c#L107">datetime_pack</a>
     * @see
     * <a href="https://github.com/tarantool/tarantool/blob/master/src/lib/core/mp_datetime.c#L56">datetime_unpack</a>
     */
    private OffsetDateTime fromBytes(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.order(LITTLE_ENDIAN);
        return Instant
            //Required part
            .ofEpochSecond(buffer.getLong())
            //Optional part
            .plusNanos(buffer.hasRemaining() ? buffer.getInt() : 0)
            .atOffset(buffer.hasRemaining() ? ZoneOffset.ofTotalSeconds(buffer.getShort() * SECONDS_PER_MINUTE) : UTC);
    }

}
