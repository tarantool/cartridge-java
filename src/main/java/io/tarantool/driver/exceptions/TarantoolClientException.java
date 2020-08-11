package io.tarantool.driver.exceptions;

/**
 * Basic exception class for client errors like connection errors, configuration error etc
 *
 * @author Alexey Kuzin
 */
public class TarantoolClientException extends TarantoolException {
    public TarantoolClientException(Throwable cause) {
        super(cause);
    }

    public TarantoolClientException(String message) {
        super(message);
    }

    public TarantoolClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public TarantoolClientException(String format, Object... args) {
        super(String.format(format, args));
    }
}
