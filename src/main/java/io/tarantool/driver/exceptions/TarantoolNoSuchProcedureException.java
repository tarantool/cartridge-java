package io.tarantool.driver.exceptions;

public class TarantoolNoSuchProcedureException extends TarantoolException {
    public TarantoolNoSuchProcedureException(String errorMessage) {
        super(errorMessage);
    }
}
