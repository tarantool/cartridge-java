package io.tarantool.driver.mappers.converters.value.defaults;

import java.io.IOException;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;

import io.tarantool.driver.mappers.converters.Adjust;
import io.tarantool.driver.mappers.converters.Interval;
import io.tarantool.driver.mappers.converters.ValueConverter;

/**
 * Default {@link ExtensionValue} to {@link Interval} converter
 *
 * @author Artyom Dubinin
 */
public class DefaultExtensionValueToIntervalConverter implements ValueConverter<ExtensionValue, Interval> {

    public static final byte EXT_TYPE = 0x06;

    public static final int FIELD_YEAR = 0;
    public static final int FIELD_MONTH = 1;
    public static final int FIELD_WEEK = 2;
    public static final int FIELD_DAY = 3;
    public static final int FIELD_HOUR = 4;
    public static final int FIELD_MIN = 5;
    public static final int FIELD_SEC = 6;
    public static final int FIELD_NSEC = 7;
    public static final int FIELD_ADJUST = 8;

    public static final int NONE_ADJUST = 0;
    public static final int EXCESS_ADJUST = 1;
    public static final int LAST_ADJUST = 2;

    private Interval fromBytes(byte[] bytes) {
        Interval interval = new Interval();
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes)) {
            int fieldsCount = unpacker.unpackInt();
            for (int i = 0; i < fieldsCount; i++) {
                int type = unpacker.unpackInt();
                switch (type) {
                    case FIELD_YEAR:
                        interval.setYear(unpacker.unpackLong());
                        break;
                    case FIELD_MONTH:
                        interval.setMonth(unpacker.unpackLong());
                        break;
                    case FIELD_WEEK:
                        interval.setWeek(unpacker.unpackLong());
                        break;
                    case FIELD_DAY:
                        interval.setDay(unpacker.unpackLong());
                        break;
                    case FIELD_HOUR:
                        interval.setHour(unpacker.unpackLong());
                        break;
                    case FIELD_MIN:
                        interval.setMin(unpacker.unpackLong());
                        break;
                    case FIELD_SEC:
                        interval.setSec(unpacker.unpackLong());
                        break;
                    case FIELD_NSEC:
                        interval.setNsec(unpacker.unpackLong());
                        break;
                    case FIELD_ADJUST:
                        long adjust = unpacker.unpackLong();
                        switch ((int) adjust) {
                            case NONE_ADJUST:
                                interval.setAdjust(Adjust.NoneAdjust);
                                break;
                            case EXCESS_ADJUST:
                                interval.setAdjust(Adjust.ExcessAdjust);
                                break;
                            case LAST_ADJUST:
                                interval.setAdjust(Adjust.LastAdjust);
                                break;
                        }
                        break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return interval;
    }

    @Override
    public Interval fromValue(ExtensionValue value) {
        return fromBytes(value.getData());
    }

    @Override
    public boolean canConvertValue(ExtensionValue value) {
        return value.getType() == EXT_TYPE;
    }
}
