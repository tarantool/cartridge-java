package io.tarantool.driver.mappers.converters.object;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import io.tarantool.driver.mappers.converters.Interval;
import io.tarantool.driver.mappers.converters.ObjectConverter;

/**
 * Default {@link Interval} to {@link ExtensionValue} converter
 *
 * @author Artyom Dubinin
 */
public class DefaultIntervalToExtensionValueConverter implements ObjectConverter<Interval, ExtensionValue> {

    private static final long serialVersionUID = 20221025L;

    public static final byte EXT_TYPE = 0x06;
    public static final int FIELD_ADJUST = 8;

    private byte[] toBytes(Interval value) {
        int fieldsCount = 0;
        List<Long> fields = Arrays.asList(value.getYear(),
            value.getMonth(),
            value.getWeek(),
            value.getDay(),
            value.getHour(),
            value.getMin(),
            value.getSec(),
            value.getNsec());
        for (long fieldValue : fields) {
            if (fieldValue != 0) {
                fieldsCount++;
            }
        }
        int adjust = value.getAdjust().ordinal();
        if (adjust != 0) {
            fieldsCount++;
        }

        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packInt(fieldsCount);

            for (int i = 0; i < fields.size(); i++) {
                packField(packer, i, fields.get(i));
            }
            if (adjust != 0) {
                packer.packInt(FIELD_ADJUST);
                packer.packLong(adjust);
            }

            return packer.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void packField(MessageBufferPacker packer, int fieldId, long fieldValue) throws IOException {
        if (fieldValue != 0) {
            packer.packInt(fieldId);
            packer.packLong(fieldValue);
        }
    }

    @Override
    public ExtensionValue toValue(Interval object) {
        return ValueFactory.newExtension(EXT_TYPE, toBytes(object));
    }
}
