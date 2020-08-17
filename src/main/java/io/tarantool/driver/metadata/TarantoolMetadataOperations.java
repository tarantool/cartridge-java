package io.tarantool.driver.metadata;

import io.tarantool.driver.exceptions.TarantoolClientException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Tarantool metadata operations interface (get space by name, get index by name, etc.)
 *
 * @author Alexey Kuzin
 */
public interface TarantoolMetadataOperations {
    /**
     * Refresh metadata cache
     * @return future with empty value for tracking the refresh progress
     * @throws TarantoolClientException if fetching data failed with error
     */
    CompletableFuture<Void> refresh() throws TarantoolClientException;

    /**
     * Get metadata for the space specified by name
     * @param spaceName the space name, must not be null or empty
     * @return nullable space metadata wrapped in {@link Optional}
     */
    Optional<TarantoolSpaceMetadata> getSpaceByName(String spaceName);

    /**
     * Get metadata for index from the specified space by name
     * @param spaceId the space ID, must be greater than 0
     * @param indexName index name, must not be null or empty
     * @return nullable index metadata wrapped in {@link Optional}
     */
    Optional<TarantoolIndexMetadata> getIndexForName(int spaceId, String indexName);

    /**
     * Get metadata for index from the specified space by index ID
     * @param spaceId the space ID, must be greater than 0
     * @param indexId index ID, must not be  must be greater or equal than 0
     * @return nullable index metadata wrapped in {@link Optional}
     */
    Optional<TarantoolIndexMetadata> getIndexForId(int spaceId, int indexId);

    /**
     * Get metadata for the space specified by id
     * @param spaceId the space ID, must be greater than 0
     * @return nullable space metadata wrapped in {@link Optional}
     */
    Optional<TarantoolSpaceMetadata> getSpaceById(int spaceId);
}
