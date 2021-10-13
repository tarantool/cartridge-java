package io.tarantool.driver.api.metadata;

import io.tarantool.driver.core.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.core.metadata.TarantoolSpaceMetadata;

import java.util.Map;

/**
 * Contains information about spaces and their indexes, parsed from an external source
 *
 * @author Alexey Kuzin
 */
public interface TarantoolMetadataContainer {
    /**
     * Get space metadata mapped to space name
     *
     * @return map of space metadata, must not be null
     */
    Map<String, TarantoolSpaceMetadata> getSpaceMetadataByName();

    /**
     * Get index metadata mapped to index name and then to space name
     *
     * @return map of index metadata, must not be null
     */
    Map<String, Map<String, TarantoolIndexMetadata>> getIndexMetadataBySpaceName();
}
