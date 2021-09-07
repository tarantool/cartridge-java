package io.tarantool.driver.exceptions;

/**
 * This exception is thrown If incorrect credentials are specified
 *
 * @author Artyom Dubinin
 */
public class TarantoolBadCredentialsException extends TarantoolClientException {
    public TarantoolBadCredentialsException() {
        super("Bad credentials");
    }
}
