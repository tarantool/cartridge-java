package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.object.DefaultUUIDToExtensionValueConverter;
import io.tarantool.driver.mappers.converters.value.DefaultExtensionValueToUUIDConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.ValueFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultUUIDConverterTest {

    @Test
    void toValue() throws IOException {
        DefaultUUIDToExtensionValueConverter converter = new DefaultUUIDToExtensionValueConverter();
        MessagePacker packer = MessagePack.newDefaultBufferPacker();
        Base64.Encoder encoder = Base64.getEncoder();
        UUID uuid = UUID.fromString("84b56906-aeed-11ea-b3de-0242ac130004");
        byte[] result = ((MessageBufferPacker) packer.packValue(converter.toValue(uuid))).toByteArray();
        assertEquals("2AKEtWkGru0R6rPeAkKsEwAE", encoder.encodeToString(result));
    }

    @Test
    void fromValue() throws IOException {
        DefaultExtensionValueToUUIDConverter converter = new DefaultExtensionValueToUUIDConverter();
        Base64.Decoder base64decoder = Base64.getDecoder();
        byte[] packed = base64decoder.decode("2AKEtWkGru0R6rPeAkKsEwAE");
        ExtensionValue value = MessagePack.newDefaultUnpacker(packed).unpackValue().asExtensionValue();
        UUID uuid = UUID.fromString("84b56906-aeed-11ea-b3de-0242ac130004");
        assertEquals(uuid, converter.fromValue(value));
    }

    @Test
    void canConvertValue() {
        DefaultExtensionValueToUUIDConverter converter = new DefaultExtensionValueToUUIDConverter();
        assertFalse(converter.canConvertValue(ValueFactory.newExtension((byte) 100, new byte[]{0})));
        assertFalse(converter.canConvertValue(ValueFactory.newExtension((byte) 0x01, new byte[]{0})));
        assertTrue(converter.canConvertValue(ValueFactory.newExtension((byte) 0x02, new byte[]{0})));
    }
}
