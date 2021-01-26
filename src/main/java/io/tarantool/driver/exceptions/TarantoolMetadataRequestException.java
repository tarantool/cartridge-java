package io.tarantool.driver.exceptions;

/**
 * The exception occurs when the metadata returned by proxy function has a wrong format or the metadata request failed
 *
 * @author Alexey Kuzin
 */
public class TarantoolMetadataRequestException extends TarantoolClientException {
    public TarantoolMetadataRequestException(String function, Throwable cause) {
        super(String.format("Failed to retrieve space and index metadata using proxy function %s", function), cause);
    }
}
