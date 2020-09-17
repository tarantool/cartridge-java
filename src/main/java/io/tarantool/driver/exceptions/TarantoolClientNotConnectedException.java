package io.tarantool.driver.exceptions;

/**
 * This exception is thrown after a request attempt using not connected client instance
 *
 * @author Alexey Kuzin
 */
public class TarantoolClientNotConnectedException extends TarantoolClientException {
    public TarantoolClientNotConnectedException() {
        super("The client is not connected to Tarantool server");
    }
}
