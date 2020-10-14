package io.tarantool.driver.metadata;

import java.util.Map;
import java.util.Optional;

/**
 * Tarantool metadata operations interface for space
 *
 * @author Sergey Volgin
 */
public interface TarantoolSpaceMetadataOperations {

    /**
     * Get metadata for index by index ID
     *
     * @param indexId index ID, must not be  must be greater or equal than 0
     * @return nullable index metadata wrapped in {@link Optional}
     */
    TarantoolIndexMetadata getIndexById(int indexId);

    /**
     * Get metadata for index by index name
     *
     * @param indexName index name, must not be  must be not empty
     * @return nullable index metadata wrapped in {@link Optional}
     */
    TarantoolIndexMetadata getIndexByName(String indexName);

    /**
     * Get metadata for all indexes of space
     *
     * @return map of index names to index metadata
     */
    Map<String, TarantoolIndexMetadata>  getSpaceIndexes();

    /**
     * Get metadata for the space
     *
     * @return nullable space metadata wrapped in {@link Optional}
     */
    TarantoolSpaceMetadata getSpaceMetadata();
}
