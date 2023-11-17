package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.object.DefaultOffsetDateTimeToExtensionValueConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultExtensionValueToOffsetDateTimeConverter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.ExtensionValue;

import java.io.IOException;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.bouncycastle.pqc.math.linearalgebra.ByteUtils.toHexString;

public class DefaultOffsetDateTimeConverterTest {

    @ParameterizedTest(name = "[{index}] {0}: [{1}] should encoded as [{2}]")
    @CsvSource({
        "Compact (contains only seconds since Epoch)" +
        " , 2023-10-23T17:45:17Z,           D7 04 2DB1366500000000",
        "Complete (has nanoseconds)" +
        " , 2023-10-23T17:45:17.1983Z,      D8 04 2DB1366500000000 60D1D10B 0000 0000",
        "Complete (has positive offset)" +
        " , 2023-10-23T20:45:17+03:00,      D8 04 2DB1366500000000 00000000 B400 0000",
        "Complete (has negative offset)" +
        " , 2023-10-23T14:45:17-03:00,      D8 04 2DB1366500000000 00000000 4CFF 0000",
        "Complete (has nanoseconds and offset)" +
        " , 2023-10-23T14:45:17.1983-03:00, D8 04 2DB1366500000000 60D1D10B 4CFF 0000",
        "Byte order check" +
        " , 2023-10-23T14:45:18.1983-03:00, D8 04 2EB1366500000000 60D1D10B 4CFF 0000",
    })
    void test_shouldReadValueWhatItWrite(
        @SuppressWarnings("unused") String description, OffsetDateTime original, String expectedNetwork
    ) throws IOException {
        DefaultOffsetDateTimeToExtensionValueConverter encoder = new DefaultOffsetDateTimeToExtensionValueConverter();
        DefaultExtensionValueToOffsetDateTimeConverter decoder = new DefaultExtensionValueToOffsetDateTimeConverter();

        assertTrue(encoder.canConvertObject(original), "Encoder should allow to encode java object");

        ExtensionValue encoded = encoder.toValue(original);
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packValue(encoded);
            String protocol = toHexString(packer.toByteArray()).toUpperCase();
            assertEquals(expectedNetwork.replace(" ", ""), protocol, "Network representation");
        }

        assertTrue(decoder.canConvertValue(encoded), "Decoder should allow to decode value");

        OffsetDateTime decoded = decoder.fromValue(encoded);
        assertEquals(original, decoded, "Decoded value should be equals to original");
    }

}
