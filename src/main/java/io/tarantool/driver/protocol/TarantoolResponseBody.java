package io.tarantool.driver.protocol;

import org.msgpack.value.Value;

/**
 * Represents Tarantool server response data frame
 *
 * @author Alexey Kuzin
 */
public interface TarantoolResponseBody {
    /**
     * Get response body type
     *
     * @return the type of response body
     */
    TarantoolResponseBodyType getResponseBodyType();

    /**
     * Get response body data
     *
     * @return a MessagePack entity
     */
    Value getData();
}
