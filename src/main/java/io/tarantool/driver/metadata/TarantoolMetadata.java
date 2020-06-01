package io.tarantool.driver.metadata;

import io.tarantool.driver.StandaloneTarantoolClient;

/**
 * Basic Tarantool spaces and indexes metadata implementation for standalone server
 */
public class TarantoolMetadata implements TarantoolMetadataOperations {
    public TarantoolMetadata(StandaloneTarantoolClient standaloneTarantoolClient) {
    }

    @Override
    public TarantoolSpaceMetadata getSpaceByName(String spaceName) {
        return null;
    }
}
