package io.tarantool.driver.exceptions;

/**
 * An exception occurs when the procedure has not (yet) been defined.
 * <p></p>
 * For example: the tarantool/crud role may not have time to rise with the necessary functions.
 * You can use this exception as a sign that it's worth trying a few more times until the role rises.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolNoSuchProcedureException extends TarantoolException {
    public TarantoolNoSuchProcedureException(String errorMessage) {
        super(errorMessage);
    }
}
