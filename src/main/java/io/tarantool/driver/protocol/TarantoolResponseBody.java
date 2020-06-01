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
     */
    public TarantoolResponseBody(int code, Value data) throws TarantoolProtocolException {
        this.responseBodyType = TarantoolResponseBodyType.fromCode(code);
        this.data = data;
    }

    /**
     * Get response body type
     * @return
     */
    public TarantoolResponseBodyType getResponseBodyType() {
        return responseBodyType;
    }

    /**
     * Get response body data
     * @return
     */
    public Value getData() {
        return data;
    }
}
