package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

/**
 * Represents a field defined by name
 *
 * @author Alexey Kuzin
 */
public interface NamedField extends FieldIdentifier<TarantoolFieldMetadata, String> {
    @Override
    TarantoolFieldMetadata metadata(
        TarantoolMetadataOperations metadataOperations,
        TarantoolSpaceMetadata spaceMetadata);

    @Override
    String toIdentifier();
}
