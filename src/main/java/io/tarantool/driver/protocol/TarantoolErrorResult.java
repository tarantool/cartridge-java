package io.tarantool.driver.protocol;

import org.msgpack.value.Value;

/**
 * Incapsulates the error data returned in Tarantool server response
 *
 * @author Alexey Kuzin
 */
public class TarantoolErrorResult {

    private final Long syncId;
    private final Long errorCode;
    private final String errorMessage;

    /**
     * Basic constructor.
     * @param syncId the request ID passed back from Tarantool server
     * @param errorCode error status code
     * @param body response body containing the error message
     */
    public TarantoolErrorResult(Long syncId, Long errorCode, Value body) throws TarantoolProtocolException {
        this.syncId = syncId;
        this.errorCode = errorCode;
        if (!body.isStringValue()) {
            throw new TarantoolProtocolException("Error body is not a MP_STRING value: {}", body.toJson());
        }
        this.errorMessage = body.asStringValue().toString();
    }

    /**
     * Get request ID a.k.a. sync ID
     * @return
     */
    public Long getSyncId() {
        return syncId;
    }

    /**
     * Get error status code
     * @return
     * @see "https://github.com/tarantool/tarantool/blob/master/src/box/errcode.h"
     */
    public Long getErrorCode() {
        return errorCode;
    }

    /**
     * Get error message
     * @return
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
