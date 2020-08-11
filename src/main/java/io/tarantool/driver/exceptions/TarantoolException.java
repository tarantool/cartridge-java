package io.tarantool.driver.exceptions;

/**
 * Base class for Tarantool runtime exceptions
 *
 * @author Alexey Kuzin
 */
public abstract class TarantoolException extends RuntimeException {
    public TarantoolException(String message) {
        super(message);
    }

    public TarantoolException() {
        super();
    }

    public TarantoolException(Throwable cause) {
        super(cause);
    }

    public TarantoolException(String message, Throwable cause) {
        super(message, cause);
    }
}
