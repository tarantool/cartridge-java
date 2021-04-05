package io.tarantool.driver.mappers;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Default {@link BigDecimal} to {@link ExtensionValue} converter
 *
 * @author Alexey Kuzin
 */
public class DefaultBigDecimalConverter implements
        ValueConverter<ExtensionValue, BigDecimal>, ObjectConverter<BigDecimal, ExtensionValue> {

    private static final byte DECIMAL_TYPE = 0x01;
    private static final int DECIMAL_MAX_DIGITS = 38;
    // See https://github.com/tarantool/decNumber/blob/master/decPacked.h
    private static final byte DECIMAL_MINUS = 0x0D;
    private static final byte DECIMAL_PLUS = 0x0C;
    private static final byte DECIMAL_MINUS_ALT = 0x0B;

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
        return packer.toByteArray();
    }

    /*
     * See https://github.com/tarantool/tarantool/blob/master/src/lib/core/decimal.c#L401
     */
    private BigDecimal fromBytes(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(buffer);
        int scale = unpacker.unpackInt();
        if (scale > DECIMAL_MAX_DIGITS || scale < -DECIMAL_MAX_DIGITS) {
            throw new IOException(
                    String.format("Scales with absolute value greater than %d are not supported", DECIMAL_MAX_DIGITS));
        }
        if (!buffer.hasRemaining()) {
            throw new IOException("Not enough bytes in the packed data");
        }
        int len = data.length;
        // Extract sign from the last nibble
        int signum = (byte) (data[len - 1] & 0x0F);
        if (signum == DECIMAL_MINUS || signum == DECIMAL_MINUS_ALT) {
            signum = -1;
        } else if (signum <= 0x09) {
            throw new IOException("The sign nibble has wrong value");
        } else {
            signum = 1;
        }
        int i;
        for (i = buffer.position() + 1; i < len && data[i] == 0; i++) {
            // skip zero bytes
        }
        if (len == i && (data[len - 1] & 0xF0) == 0) {
            return BigDecimal.ZERO;
        }

        int digitsNum = (len - i) << 1;
        char digit = (char) ((data[len - 1] & 0xF0) >>> 4);
        if (digit > 9) {
            throw new IOException(String.format("Invalid digit at position %d", digitsNum - 1));
        }
        char[] digits = new char[digitsNum];
        int pos = 2 * (len - i) - 1;
        digits[pos--] = Character.forDigit(digit, 10);
        for (int j = len - 2; j >= i; j--) {
            digit = (char) (data[j] & 0x0F);
            if (digit > 9) {
                throw new IOException(String.format("Invalid digit at position %d", pos));
            }
            digits[pos--] = Character.forDigit(digit, 10);
            digit = (char) ((data[j] & 0xF0) >>> 4);
            if (digit > 9) {
                throw new IOException(String.format("Invalid digit at position %d", pos - 1));
            }
            digits[pos--] = Character.forDigit(digit, 10);
        }
        StringBuilder sb = new StringBuilder(len - i + 1);
        if (signum < 0) {
            sb.append('-');
        }
        pos = 0;
        while (digits[pos] == 0) {
            pos++;
        }
        for (; pos < digits.length; pos++) {
            sb.append(digits[pos]);
        }
        return new BigDecimal(new BigInteger(sb.toString()), scale);
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

    @Override
    public BigDecimal fromValue(ExtensionValue value) {
        try {
            return fromBytes(value.getData());
        } catch (IOException e) {
            throw new MessagePackValueMapperException(
                    String.format("Failed to unpack BigDecimal from MessagePack entity %s", value.toString()), e);
        }
    }

    @Override
    public boolean canConvertValue(ExtensionValue value) {
        return value.getType() == DECIMAL_TYPE;
    }
}
