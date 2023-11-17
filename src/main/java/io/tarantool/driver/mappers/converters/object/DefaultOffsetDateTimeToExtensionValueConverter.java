package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;

import static io.tarantool.driver.mappers.converters.value.defaults.DefaultExtensionValueToInstantConverter.DATETIME_TYPE;
import static io.tarantool.driver.mappers.converters.value.defaults.DefaultExtensionValueToOffsetDateTimeConverter.SECONDS_PER_MINUTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.time.ZoneOffset.UTC;

/**
 * Default {@link ExtensionValue} to {@link java.time.OffsetDateTime} converter.
 *
 * @author Valeriy Vyrva
 */
public class DefaultOffsetDateTimeToExtensionValueConverter implements ObjectConverter<OffsetDateTime, ExtensionValue> {

    private static final long serialVersionUID = 20231027114017L;

    /**
     * Will contain only requited part:
     * <ol>
     * <li>{@code 8 bytes}: Seconds since Epoch.</li>
     * </ol>
     *
     * @see <a href="https://github.com/tarantool/tarantool/blob/master/src/lib/core/datetime.h#L85">struct datetime</a>
     */
    private static final int BUFFER_SIZE_COMPACT = Long.BYTES;
    /**
     * Will contain and required and optional parts:
     * <ol>
     * <li>{@code 8 bytes}: Seconds since Epoch.</li>
     * <li>{@code 4 bytes}: Nanoseconds.</li>
     * <li>{@code 2 bytes}: Offset in minutes from UTC.</li>
     * <li>{@code 2 bytes}: Olson timezone id.</li>
     * </ol>
     * The "timezone id" is not used on Java.
     *
     * @see <a href="https://github.com/tarantool/tarantool/blob/master/src/lib/core/datetime.h#L85">struct datetime</a>
     */
    private static final int BUFFER_SIZE_COMPLETE = Long.BYTES + Integer.BYTES + Short.BYTES + Short.BYTES;

    @Override
    public ExtensionValue toValue(OffsetDateTime object) {
        return ValueFactory.newExtension(DATETIME_TYPE, toBytes(object));
    }

    /**
     * Encode java object into protocol level representation.
     *
     * @param object Object to encode
     * @return Protocol level representation
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
    private byte[] toBytes(OffsetDateTime object) {
        boolean isCompact = object.getNano() == 0 && object.getOffset().equals(UTC);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[isCompact ? BUFFER_SIZE_COMPACT : BUFFER_SIZE_COMPLETE]);
        buffer.order(LITTLE_ENDIAN);
        //Required part
        buffer.putLong(object.toEpochSecond());
        //Optional part
        if (!isCompact) {
            buffer.putInt(object.getNano());
            buffer.putShort((short) (object.getOffset().getTotalSeconds() / SECONDS_PER_MINUTE));
        }
        return buffer.array();
    }

}
