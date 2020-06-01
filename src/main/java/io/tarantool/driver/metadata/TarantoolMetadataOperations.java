package io.tarantool.driver.metadata;

/**
 * Tarantool metadata operations interface (get space by name, get index by name, etc.)
 *
 * @author Alexey Kuzin
 */
public interface TarantoolMetadataOperations {
    TarantoolSpaceMetadata getSpaceByName(String spaceName);
}
