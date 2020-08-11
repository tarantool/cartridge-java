package io.tarantool.driver.exceptions;

/**
 * Base class for Tarantool runtime exceptions
 *
 * @author Alexey Kuzin
 */
public abstract class TarantoolRuntimeException extends RuntimeException {
    public TarantoolRuntimeException(String message) {
        super(message);
    }

    public TarantoolRuntimeException() {
        super();
    }
}
