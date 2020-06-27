package io.tarantool.driver.protocol;

import org.msgpack.value.Value;

/**
 * Represents empty body from a map no entries. It is expected as a response for several special requests
 *
 * @author Alexey Kuzin
 */
public class EmptyTarantoolResponseBody implements TarantoolResponseBody {
    private final TarantoolResponseBodyType responseBodyType;

    /**
     * Basic constructor.
     */
    public EmptyTarantoolResponseBody() {
        this.responseBodyType = TarantoolResponseBodyType.EMPTY;
    }

    @Override
    public TarantoolResponseBodyType getResponseBodyType() {
        return responseBodyType;
    }

    @Override
    public Value getData() {
        throw new UnsupportedOperationException("Response body is empty");
    }
}
