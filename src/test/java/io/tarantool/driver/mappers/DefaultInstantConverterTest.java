package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.object.DefaultInstantToExtensionValueConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultExtensionValueToInstantConverter;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ImmutableExtensionValue;
import org.msgpack.value.ValueFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultInstantConverterTest {
    @Test
    void toValue() throws IOException {
        DefaultInstantToExtensionValueConverter converter = new DefaultInstantToExtensionValueConverter();
        MessagePacker packer = MessagePack.newDefaultBufferPacker();
        Base64.Encoder encoder = Base64.getEncoder();
        Instant instant = LocalDateTime.parse("2022-10-25T12:03:58").toInstant(ZoneOffset.UTC);
        byte[] result = ((MessageBufferPacker) packer.packValue(converter.toValue(instant))).toByteArray();
        assertEquals("1wSu0FdjAAAAAA==", encoder.encodeToString(result));
        packer.close();
    }

    @Test
    void fromValue() throws IOException {
        DefaultExtensionValueToInstantConverter converter = new DefaultExtensionValueToInstantConverter();
        Base64.Decoder base64decoder = Base64.getDecoder();
        Instant instant = LocalDateTime.parse("2022-10-25T12:03:58").toInstant(ZoneOffset.UTC);
        byte[] packed = base64decoder.decode("2ASu0FdjAAAAAAAAAAAAAAAA");
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packed);
        ExtensionValue value = unpacker.unpackValue().asExtensionValue();
        assertEquals(instant, converter.fromValue(value));
        unpacker.close();
    }

    @Test
    void canConvertValue() {
        DefaultExtensionValueToInstantConverter converter = new DefaultExtensionValueToInstantConverter();
        assertFalse(converter.canConvertValue(ValueFactory.newExtension((byte) 100, new byte[]{0})));
        assertFalse(converter.canConvertValue(ValueFactory.newExtension((byte) 0x01, new byte[]{0})));
        ImmutableExtensionValue value = ValueFactory.newExtension((byte) 0x04, new byte[]{0});
        assertTrue(converter.canConvertValue(value));
    }
}
