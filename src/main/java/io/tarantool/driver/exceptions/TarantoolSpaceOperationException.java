package io.tarantool.driver.exceptions;

/**
 * Basic exception class for space operations errors like select, insert, replace etc
 *
 * @author Sergey Volgin
 */
public class TarantoolSpaceOperationException extends TarantoolClientException {
    public TarantoolSpaceOperationException(Throwable cause) {
        super(cause);
    }

    public TarantoolSpaceOperationException(String message) {
        super(message);
    }

    public TarantoolSpaceOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public TarantoolSpaceOperationException(String format, Object... args) {
        super(format, args);
    }
}
