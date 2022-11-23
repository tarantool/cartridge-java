package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

/**
 * Represents an index defined by its id
 *
 * @author Alexey Kuzin
 */
public interface IdIndex extends FieldIdentifier<TarantoolIndexMetadata, Integer> {
    @Override
    TarantoolIndexMetadata metadata(
        TarantoolMetadataOperations metadataOperations,
        TarantoolSpaceMetadata spaceMetadata);

    @Override
    Integer toIdentifier();
}
