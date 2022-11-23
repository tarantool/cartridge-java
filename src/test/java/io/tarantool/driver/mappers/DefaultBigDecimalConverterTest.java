package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.object.DefaultBigDecimalToExtensionValueConverter;
import io.tarantool.driver.mappers.converters.value.DefaultExtensionValueToBigDecimalConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultBigDecimalConverterTest {

    @Test
    void toValue() throws IOException {
        DefaultBigDecimalToExtensionValueConverter converter = new DefaultBigDecimalToExtensionValueConverter();
        MessagePacker packer = MessagePack.newDefaultBufferPacker();
        Base64.Encoder encoder = Base64.getEncoder();
        byte[] result = ((MessageBufferPacker) packer.packValue(converter.toValue(BigDecimal.ONE))).toByteArray();
        assertEquals("1QEAHA==", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(converter.toValue(BigDecimal.ZERO))).toByteArray();
        assertEquals("1QEADA==", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(converter.toValue(new BigDecimal(-1)))).toByteArray();
        assertEquals("1QEAHQ==", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(converter.toValue(new BigDecimal(1111111)))).toByteArray();
        assertEquals("xwUBABERERw=", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(
            converter.toValue(new BigDecimal(1111111111111111111L)))).toByteArray();
        assertEquals("xwsBABERERERERERERw=", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(
            converter.toValue(new BigDecimal("1111111111111111111111111")))).toByteArray();
        assertEquals("xw4BABERERERERERERERERw=", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(
            converter.toValue(BigDecimal.valueOf(1.2)))).toByteArray();
        assertEquals("xwMBAQEs", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(
            converter.toValue(new BigDecimal("1111111111111.111111111111")))).toByteArray();
        assertEquals("xw4BDBERERERERERERERERw=", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(
            converter.toValue(BigDecimal.valueOf(-1.2)))).toByteArray();
        assertEquals("xwMBAQEt", encoder.encodeToString(result));
        packer = MessagePack.newDefaultBufferPacker();
        result = ((MessageBufferPacker) packer.packValue(
            converter.toValue(new BigDecimal("-1111111111111.111111111111")))).toByteArray();
        assertEquals("xw4BDBERERERERERERERER0=", encoder.encodeToString(result));
    }

    @Test
    void fromValue() throws IOException {
        DefaultExtensionValueToBigDecimalConverter converter = new DefaultExtensionValueToBigDecimalConverter();
        Base64.Decoder base64decoder = Base64.getDecoder();
        byte[] mpOne = base64decoder.decode("1QEAHA==");
        ExtensionValue value = MessagePack.newDefaultUnpacker(mpOne).unpackValue().asExtensionValue();
        assertEquals(BigDecimal.ONE, converter.fromValue(value));
        byte[] mpZero = base64decoder.decode("1QEADA==");
        value = MessagePack.newDefaultUnpacker(mpZero).unpackValue().asExtensionValue();
        assertEquals(BigDecimal.ZERO, converter.fromValue(value));
        byte[] mpMinusOne = base64decoder.decode("1QEAHQ==");
        value = MessagePack.newDefaultUnpacker(mpMinusOne).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal(-1), converter.fromValue(value));
        byte[] mpSmall = base64decoder.decode("xwUBABERERw=");
        value = MessagePack.newDefaultUnpacker(mpSmall).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal(1111111), converter.fromValue(value));
        byte[] mpBig = base64decoder.decode("xwsBABERERERERERERw=");
        value = MessagePack.newDefaultUnpacker(mpBig).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal(1111111111111111111L), converter.fromValue(value));
        byte[] mpVeryBig = base64decoder.decode("xw4BABERERERERERERERERw=");
        value = MessagePack.newDefaultUnpacker(mpVeryBig).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal("1111111111111111111111111"), converter.fromValue(value));
        byte[] mpFloat = base64decoder.decode("xwMBAQEs");
        value = MessagePack.newDefaultUnpacker(mpFloat).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal("1.2"), converter.fromValue(value));
        byte[] mpFloatBig = base64decoder.decode("xw4BDRERERERERERERERERw=");
        value = MessagePack.newDefaultUnpacker(mpFloatBig).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal("111111111111.1111111111111"), converter.fromValue(value));
        byte[] mpFloatNeg = base64decoder.decode("xwMBAQEt");
        value = MessagePack.newDefaultUnpacker(mpFloatNeg).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal("-1.2"), converter.fromValue(value));
        byte[] mpFloatBigNeg = base64decoder.decode("xw4BDRERERERERERERERER0=");
        value = MessagePack.newDefaultUnpacker(mpFloatBigNeg).unpackValue().asExtensionValue();
        assertEquals(new BigDecimal("-111111111111.1111111111111"), converter.fromValue(value));
    }

    @Test
    void canConvertValue() {
        DefaultExtensionValueToBigDecimalConverter converter = new DefaultExtensionValueToBigDecimalConverter();
        assertFalse(converter.canConvertValue(ValueFactory.newExtension((byte) 100, new byte[]{0})));
        assertTrue(converter.canConvertValue(ValueFactory.newExtension((byte) 0x01, new byte[]{0})));
    }
}
