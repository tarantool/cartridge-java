package io.tarantool.driver;

/**
 * Basic exception class for client errors like connection errors, configuration error etc
 */
public class TarantoolClientException extends Throwable {
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
