package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

/**
 * Represents a field defined by position
 *
 * @author Alexey Kuzin
 */
public interface PositionField extends FieldIdentifier<TarantoolFieldMetadata, Integer> {
    @Override
    TarantoolFieldMetadata metadata(
        TarantoolMetadataOperations metadataOperations,
        TarantoolSpaceMetadata spaceMetadata);

    @Override
    Object toIdentifier();
}
