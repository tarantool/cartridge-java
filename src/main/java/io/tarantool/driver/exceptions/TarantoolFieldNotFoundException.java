package io.tarantool.driver.exceptions;

import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

/**
 * Represents error when a field is not found in space format metadata
 *
 * @author Alexey Kuzin
 */
public class TarantoolFieldNotFoundException extends TarantoolClientException {

    public TarantoolFieldNotFoundException(int position, TarantoolSpaceMetadata spaceMetadata) {
        super("Field %d not found in space %s", position, spaceMetadata.getSpaceName());
    }

    public TarantoolFieldNotFoundException(String name, TarantoolSpaceMetadata spaceMetadata) {
        super("Field %d not found in space %s", name, spaceMetadata.getSpaceName());
    }
}
