package io.tarantool.driver.exceptions;

/**
 * Corresponds to an exception that occurs when the procedure has not (yet) been defined in the Tarantool instance.
 *
 * For example: this exception can be raised when a Cartridge role is not up yet or is being reloaded at the moment.
 * This can happen when the tarantool 'router' node is restarted or hot-reloaded but the cluster is under
 * load at the moment. If CRUD module is used in the cluster then 'cartridge.roles.crud-router' may
 * have not been initialized yet and all requests to CRUD will fail with this exception.
 *
 * You can use this exception when you are sure that it's worth trying a few more times until the role is up.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolNoSuchProcedureException extends TarantoolException {
    public TarantoolNoSuchProcedureException(String errorMessage) {
        super(errorMessage);
    }
}
