package io.tarantool.driver.exceptions;

/**
 * Represents an error where all connections in a pool are closed
 *
 * @author Alexey Kuzin
 */
public class NoAvailableConnectionsException extends TarantoolClientException {

    /**
     * Basic constructor.
     */
    public NoAvailableConnectionsException() {
        super("No available connections");
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
