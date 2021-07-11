package io.tarantool.driver.exceptions;

/**
 * Represents server internal error subclasses that are related to the network problems
 * with connections between Tarantool nodes or external services accessed
 * from inside Tarantool (Connection timeout, No connection, etc.)
 *
 * @author Artyom Dubinin
 */
public class TarantoolServerInternalNetworkException extends TarantoolServerInternalException {
    public TarantoolServerInternalNetworkException(String errorMessage) {
        super(errorMessage);
    }
}
