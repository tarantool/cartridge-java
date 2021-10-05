package io.tarantool.driver.exceptions;

public class TarantoolBadClientTypeException extends TarantoolClientException {
    public TarantoolBadClientTypeException() {
        super("Unsupported client type");
    }

    public TarantoolBadClientTypeException(Class<?> clazz) {
        super(String.format("Unsupported client type %s", clazz.getName()));
    }
}
