package io.tarantool.driver.exceptions;

/**
 * An exception occurs when the procedure has not (yet) been defined.
 * <p></p>
 * For example: this exception can be raised when a role is not up yet or being reloaded at the moment. This can happen when the tarantool 'router' node is restarted or hot-reloaded but the cluster is under load at the moment. If CRUD module is used in the cluster then 'cartridge.roles.crud-router' may have not been initialized yet and all requests to CRUD will fail with this exception.
 * You can use this exception as a sign that it's worth trying a few more times until the role is up.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolNoSuchProcedureException extends TarantoolException {
    public TarantoolNoSuchProcedureException(String errorMessage) {
        super(errorMessage);
    }
}
