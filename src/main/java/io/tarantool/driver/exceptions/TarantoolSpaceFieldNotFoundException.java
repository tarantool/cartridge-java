package io.tarantool.driver.exceptions;

/**
 * Field not found in current space error
 *
 *  @author Sergey Volgin
 */
public class TarantoolSpaceFieldNotFoundException extends TarantoolException {
    public TarantoolSpaceFieldNotFoundException(String fieldName) {
        super(String.format("Field \"%s\" not found in space format metadata", fieldName));
    }
}
