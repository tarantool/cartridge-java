package io.tarantool.driver.exceptions;

/**
 * Represents exceptions returned for call operations, if the server response does not match the expected format
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class TarantoolFunctionCallException extends TarantoolException {
    public TarantoolFunctionCallException(String errorMessage) {
        super(errorMessage);
    }
}
