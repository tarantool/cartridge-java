package io.tarantool.driver.exceptions;

/**
 * Corresponds to an exception that occurs when the connected user doesn't have access to function, space or sequence.
 * <p>
 * For example: this exception can be raised when a user doesn't have access
 * to metadata functions(ddl.get_schema, _vspace:select)
 *
 * @author Artyom Dubinin
 * @see <a href="https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_error/">box_error</a>
 */
public class TarantoolAccessDeniedException extends TarantoolException {
    public TarantoolAccessDeniedException(String errorMessage) {
        super(errorMessage);
    }
}
