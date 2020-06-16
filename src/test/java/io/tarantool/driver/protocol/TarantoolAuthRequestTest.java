package io.tarantool.driver.protocol;

import io.tarantool.driver.auth.TarantoolAuthMechanism;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.protocol.requests.TarantoolAuthRequest;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TarantoolAuthRequestTest {

    @Test
    void createAndSerialize() throws Exception {
        assertThrows(TarantoolProtocolException.class,
                () -> new TarantoolAuthRequest.Builder().withUsername("user").build(),
                "Username and auth data must be specified for Tarantool auth request");
        assertThrows(IllegalArgumentException.class,
                () -> new TarantoolAuthRequest.Builder().withUsername(null).build(),
                "Username must not be empty");
        assertThrows(IllegalArgumentException.class,
                () -> new TarantoolAuthRequest.Builder()
                        .withAuthData(TarantoolAuthMechanism.CHAPSHA1, null).build(),
                "Auth data must not be empty");
        TarantoolAuthRequest request = new TarantoolAuthRequest.Builder()
                .withUsername("user")
                .withAuthData(TarantoolAuthMechanism.CHAPSHA1, new byte[]{1, 2, 3, 4}).build();
        MessagePacker packer = MessagePack.newDefaultBufferPacker();
        request.toMessagePack(packer, DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper());
        packer.flush();
        byte[] bytes = ((MessageBufferPacker) packer).toByteArray();
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
        // check header
        TarantoolHeader authHeader = TarantoolHeader.fromMessagePackValue(unpacker.unpackValue());
        assertEquals(TarantoolRequestType.IPROTO_AUTH.getCode(), authHeader.getCode());
        assertEquals(1, authHeader.getSync());
        // check body
        Value value = unpacker.unpackValue();
        assertTrue(value.isMapValue());
        Map<Value, Value> values = value.asMapValue().map();
        assertEquals(2, values.size());
        assertEquals("user", values.get(ValueFactory.newInteger(0x23)).asStringValue().asString());
        Value authArray = values.get(ValueFactory.newInteger(0x21));
        assertTrue(authArray.isArrayValue());
        assertEquals("chap-sha1", authArray.asArrayValue().get(0).asStringValue().asString());
        assertArrayEquals(new byte[]{1, 2, 3, 4}, authArray.asArrayValue().get(1).asBinaryValue().asByteArray());
    }
}
