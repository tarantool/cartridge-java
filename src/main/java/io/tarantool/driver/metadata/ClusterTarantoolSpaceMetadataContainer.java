package io.tarantool.driver.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is not part of the public API.
 *
 * @author Sergey Volgin
 */
public class ClusterTarantoolSpaceMetadataContainer {

    private Map<String, TarantoolSpaceMetadata> spaceMetadata = new HashMap<>();
    private Map<String, Map<String, TarantoolIndexMetadata>> indexMetadata = new HashMap<>();

    public ClusterTarantoolSpaceMetadataContainer() {
    }

    public Map<String, TarantoolSpaceMetadata> getSpaceMetadata() {
        return spaceMetadata;
    }

    public Map<String, Map<String, TarantoolIndexMetadata>> getIndexMetadata() {
        return indexMetadata;
    }

    public void addSpace(TarantoolSpaceMetadata metadata) {
        spaceMetadata.put(metadata.getSpaceName(), metadata);
    }

    public void addIndexes(String spaceName, Map<String, TarantoolIndexMetadata> indexMetadataMap) {
        indexMetadata.put(spaceName, indexMetadataMap);
    }
}
