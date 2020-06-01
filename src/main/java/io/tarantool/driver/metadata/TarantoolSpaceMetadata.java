package io.tarantool.driver.metadata;

/**
 * Represents Tarantool space metadata (space ID, space name, etc.)
 *
 * @author Alexey Kuzin
 */
public interface TarantoolSpaceMetadata {
    /**
     * Get space ID on the Tarantool server
     * @return a number
     */
    int getSpaceId();
}
