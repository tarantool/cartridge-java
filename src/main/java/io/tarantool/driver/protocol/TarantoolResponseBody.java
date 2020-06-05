package io.tarantool.driver.protocol;

import org.msgpack.value.Value;

/**
 * Represents Tarantool server response data data frame
 *
 * @author Alexey Kuzin
 */
public class TarantoolResponseBody {
    private final TarantoolResponseBodyType responseBodyType;
    private final Value data;

    /**
     * Basic constructor.
     * @param code first key in the body MP_MAP value
     * @param data the data (of type MP_OBJECT)
     * @throws TarantoolProtocolException if the specified code doesn't correspond to a valid {@link TarantoolResponseBodyType}
     */
    public TarantoolResponseBody(int code, Value data) throws TarantoolProtocolException {
        this.responseBodyType = TarantoolResponseBodyType.fromCode(code);
        this.data = data;
    }

    /**
     * Get response body type
     * @return the type of response body
     */
    public TarantoolResponseBodyType getResponseBodyType() {
        return responseBodyType;
    }

    /**
     * Get response body data
     * @return a MessagePack entity
     */
    public Value getData() {
        return data;
    }
}
