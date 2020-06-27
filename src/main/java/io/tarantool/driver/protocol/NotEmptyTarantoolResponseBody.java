package io.tarantool.driver.protocol;

import org.msgpack.value.Value;

/**
 * Represents non-empty body from a map with one key and the actual data as value
 *
 * @author Alexey Kuzin
 */
public class NotEmptyTarantoolResponseBody implements TarantoolResponseBody {
    private final TarantoolResponseBodyType responseBodyType;
    private final Value data;

    /**
     * Basic constructor.
     * @param code first key in the body MP_MAP value
     * @param data the data (of type MP_OBJECT)
     * @throws TarantoolProtocolException if the specified code doesn't correspond to a valid
     * {@link TarantoolResponseBodyType}
     */
    public NotEmptyTarantoolResponseBody(int code, Value data) throws TarantoolProtocolException {
        this.responseBodyType = TarantoolResponseBodyType.fromCode(code);
        this.data = data;
    }

    @Override
    public TarantoolResponseBodyType getResponseBodyType() {
        return responseBodyType;
    }

    @Override
    public Value getData() {
        return data;
    }
}
