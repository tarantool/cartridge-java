package io.tarantool.driver.exceptions;

/**
 * This exception is thrown after a request attempt using not connected client instance
 *
 * @author Alexey Kuzin
 */
public class TarantoolConnectionException extends TarantoolClientException {
    public TarantoolConnectionException(Throwable ex) {
        super("The client is not connected to Tarantool server", ex);
    }
}
