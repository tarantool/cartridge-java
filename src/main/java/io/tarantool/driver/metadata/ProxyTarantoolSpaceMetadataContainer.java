package io.tarantool.driver.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is not part of the public API.
 *
 * @author Sergey Volgin
 */
public class ProxyTarantoolSpaceMetadataContainer {

    private final Map<String, TarantoolSpaceMetadata> spaceMetadata = new HashMap<>();
    private final Map<String, Map<String, TarantoolIndexMetadata>> indexMetadata = new HashMap<>();

    public ProxyTarantoolSpaceMetadataContainer() {
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
