package io.tarantool.driver.protocol;

import org.msgpack.value.Value;

/**
 * Incapsulates the result data returned in Tarantool server response
 *
 * @author Alexey Kuzin
 */
public class TarantoolOkResult {

    private final Long syncId;
    private final Value data;

    /**
     * Basic constructor.
     * @param syncId the request ID passed back from Tarantool server
     * @param body response body containing the result data
     */
    public TarantoolOkResult(Long syncId, Value body) {
        this.syncId = syncId;
        this.data = body;
    }

    /**
     * Get request ID a.k.a. sync ID
     * @return a number
     */
    public Long getSyncId() {
        return syncId;
    }

    /**
     * Get response data
     * @return a MessagePack entity
     */
    public Value getData() {
        return data;
    }
}
