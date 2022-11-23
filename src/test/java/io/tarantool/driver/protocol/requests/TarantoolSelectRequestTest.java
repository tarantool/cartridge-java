package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.exceptions.TarantoolDecoderException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * @author Alexey Kuzin
 */
public class TarantoolSelectRequestTest {

    private final MessagePackMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private final MessagePacker packer = MessagePack.newDefaultBufferPacker();

    @Test
    public void testNullKeyValues() throws TarantoolProtocolException, IOException {
        TarantoolSelectRequest request = new TarantoolSelectRequest.Builder()
            .withSpaceId(512)
            .withIndexId(0)
            .withOffset(0)
            .withLimit(0xff_ff_ff_ffL)
            .withIteratorType(TarantoolIteratorType.ITER_ALL)
            .withKeyValues(Arrays.asList(null, 123))
            .build(mapper);

        List<Value> expected = Arrays.asList(ValueFactory.newNil(), ValueFactory.newInteger(123));
        assertArrayEquals(buildSelectRequest(expected, request.getHeader().getSync()), selectRequestToBytes(request));
    }

    private byte[] selectRequestToBytes(TarantoolSelectRequest request) throws TarantoolDecoderException {
        try {
            request.toMessagePack(packer, mapper);
            return ((MessageBufferPacker) packer).toByteArray();
        } finally {
            packer.clear();
        }
    }

    private byte[] buildSelectRequest(List<Value> keyValues, long sync) throws IOException {
        Map<IntegerValue, Value> bodyMap = new HashMap<>();
        bodyMap.put(
            ValueFactory.newInteger(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode()),
            ValueFactory.newInteger(512));
        bodyMap.put(
            ValueFactory.newInteger(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode()),
            ValueFactory.newInteger(0));
        bodyMap.put(
            ValueFactory.newInteger(TarantoolRequestFieldType.IPROTO_OFFSET.getCode()),
            ValueFactory.newInteger(0));
        bodyMap.put(
            ValueFactory.newInteger(TarantoolRequestFieldType.IPROTO_LIMIT.getCode()),
            ValueFactory.newInteger(0xff_ff_ff_ffL));
        bodyMap.put(
            ValueFactory.newInteger(TarantoolRequestFieldType.IPROTO_ITERATOR.getCode()),
            ValueFactory.newInteger(TarantoolIteratorType.ITER_ALL.getCode()));
        bodyMap.put(
            ValueFactory.newInteger(TarantoolRequestFieldType.IPROTO_KEY.getCode()),
            ValueFactory.newArray(keyValues));
        MapValue body = ValueFactory.newMap(bodyMap);

        Map<IntegerValue, IntegerValue> headerMap = new HashMap<>();
        headerMap.put(ValueFactory.newInteger(0x00), ValueFactory.newInteger(1));
        headerMap.put(ValueFactory.newInteger(0x01), ValueFactory.newInteger(sync));
        MapValue header = ValueFactory.newMap(headerMap);

        packer.packValue(header);
        packer.packValue(body);

        try {
            return ((MessageBufferPacker) packer).toByteArray();
        } finally {
            packer.clear();
        }
    }
}
