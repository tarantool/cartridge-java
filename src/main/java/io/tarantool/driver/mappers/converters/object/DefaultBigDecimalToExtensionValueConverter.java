package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.MessagePackValueMapperException;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Default {@link BigDecimal} to {@link ExtensionValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultBigDecimalToExtensionValueConverter implements ObjectConverter<BigDecimal, ExtensionValue> {

    private static final long serialVersionUID = 20220418L;

    private static final byte DECIMAL_TYPE = 0x01;
    private static final int DECIMAL_MAX_DIGITS = 38;
    // See https://github.com/tarantool/decNumber/blob/master/decPacked.h
    private static final byte DECIMAL_MINUS = 0x0D;
    private static final byte DECIMAL_PLUS = 0x0C;

    private byte[] toBytes(BigDecimal object) throws IOException {
        int scale = object.scale();
        if (scale > DECIMAL_MAX_DIGITS || scale < -DECIMAL_MAX_DIGITS) {
            throw new IOException(
                    String.format("Scales with absolute value greater than %d are not supported", DECIMAL_MAX_DIGITS));
        }
        String number = object.unscaledValue().toString();
        byte signum = DECIMAL_PLUS;
        int digitsNum = number.length();
        int pos = 0;
        if (number.charAt(0) == '-') {
            signum = DECIMAL_MINUS;
            digitsNum--;
            pos++;
        }
        int len = (digitsNum >> 1) + 1;
        byte[] bcd = new byte[len];
        bcd[len - 1] = signum;
        char[] digits = number.substring(pos).toCharArray();
        pos = digits.length - 1;
        for (int i = len - 1; i > 0; i--) {
            bcd[i] |= Character.digit(digits[pos--], 10) << 4;
            bcd[i - 1] |= Character.digit(digits[pos--], 10);
        }
        if (pos == 0) {
            bcd[0] |= Character.digit(digits[pos], 10) << 4;
        }
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packInt(scale);
        packer.writePayload(bcd);
        packer.close();
        return packer.toByteArray();
    }

    @Override
    public ExtensionValue toValue(BigDecimal object) {
        try {
            return ValueFactory.newExtension(DECIMAL_TYPE, toBytes(object));
        } catch (IOException e) {
            throw new MessagePackValueMapperException(
                    String.format("Failed to pack BigDecimal %s to MessagePack entity", object), e);
        }
    }
}
