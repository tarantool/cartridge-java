package io.tarantool.driver.protocol;

import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapperException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents basic Tarantool request body
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestBody implements Packable<MapValue> {

    private Map<IntegerValue, Value> values;

    /**
     * In rare cases, the body may be empty. Creates a request with empty body
     */
    public TarantoolRequestBody() {
        values = Collections.emptyMap();
    }

    /**
     * Uses default {@link MessagePackObjectMapper} for converting values.
     * @param body request body
     * @throws TarantoolProtocolException in case if mapping of body parts to objects failed
     * @see #TarantoolRequestBody(Map, MessagePackObjectMapper)
     */
    public TarantoolRequestBody(Map<Integer, ?> body) throws TarantoolProtocolException {
        this(body, DefaultMessagePackMapper.getInstance());
    }

    /**
     * Basic constructor. Takes a typical {@link Map} with {@code Integer} keys and {@code Object} values.
     * Converts values into MessagePack entities using the passed instance of {@link MessagePackObjectMapper}.
     * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
     * @param body request body
     * @param mapper provides mapping for Java objects to MessagePack entities
     * @throws TarantoolProtocolException in case if mapping of body parts to objects failed
     */
    public TarantoolRequestBody(Map<Integer, ?> body, MessagePackObjectMapper mapper) throws TarantoolProtocolException {
        try {
            this.values = new HashMap<>(body.size(), 1);
            for (Integer key: body.keySet()) {
                values.put(ValueFactory.newInteger(key), mapper.toValue(body.get(key)));
            }
        } catch (MessagePackValueMapperException e) {
            throw new TarantoolProtocolException(e);
        }
    }

    @Override
    public MapValue toMessagePackValue(MessagePackObjectMapper mapper) {
        return ValueFactory.newMap(values);
    }
}
