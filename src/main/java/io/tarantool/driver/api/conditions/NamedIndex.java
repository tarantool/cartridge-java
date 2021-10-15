package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

/**
 * Represents an index defined by name
 *
 * @author Alexey Kuzin
 */
public interface NamedIndex extends FieldIdentifier<TarantoolIndexMetadata, String> {
    @Override
    TarantoolIndexMetadata metadata(TarantoolMetadataOperations metadataOperations,
                                    TarantoolSpaceMetadata spaceMetadata);

    @Override
    String toIdentifier();
}
