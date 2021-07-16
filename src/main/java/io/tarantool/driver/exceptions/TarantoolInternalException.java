package io.tarantool.driver.exceptions;

/**
 * Represents exceptions returned on call operations from
 * Lua API (functions return <code>nil, err</code> or the `error()` function is called in the function body)
 *
 * @author Artyom Dubinin
 */
public class TarantoolInternalException extends TarantoolException {
    public TarantoolInternalException(String errorMessage) {
        super(errorMessage);
    }
}
