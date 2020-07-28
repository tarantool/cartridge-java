package io.tarantool.driver.exceptions;

public class TarantoolSpaceFieldNotFoundException extends RuntimeException {
    public TarantoolSpaceFieldNotFoundException(String fieldName) {
        super(String.format("Field \"%s\" not found in space format metadata", fieldName));
    }
}
