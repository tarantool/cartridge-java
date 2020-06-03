package io.tarantool.driver.exceptions;

import io.tarantool.driver.TarantoolClientException;

/**
 * Represents space not found error
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpaceNotFoundException extends TarantoolClientException {
    public TarantoolSpaceNotFoundException(int spaceId) {
        super(String.format("Space with id %d not found", spaceId));
    }

    public TarantoolSpaceNotFoundException(String spaceName) {
        super(String.format("Space with name '%s' not found", spaceName));
    }
}
