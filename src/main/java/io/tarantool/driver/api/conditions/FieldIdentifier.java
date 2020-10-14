package io.tarantool.driver.api.conditions;

import io.tarantool.driver.metadata.TarantoolSpaceMetadataOperations;

/**
 * Represents filtering operand in conditions, it may be an index or a field
 *
 * @param <T> metadata type
 * @author Alexey Kuzin
 */
public interface FieldIdentifier<T, O> {
    /**
     * Returns metadata object corresponding to the field identifier type
     *
     * @param spaceMetadataOperations for retrieving the operand metadata and checking the filed or index availability
     * @return name to be used in filter condition
     */
    T metadata(TarantoolSpaceMetadataOperations spaceMetadataOperations);

    /**
     * Get serializable form of the identifier
     *
     * @return an object serializable into MessagePack
     */
    O toIdentifier();
}
