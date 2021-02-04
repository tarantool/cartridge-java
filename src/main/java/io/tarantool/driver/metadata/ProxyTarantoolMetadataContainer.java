package io.tarantool.driver.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains spaces and indexes metadata information retrieved from a call to a stored function
 *
 * @author Sergey Volgin
 */
public class ProxyTarantoolMetadataContainer implements TarantoolMetadataContainer {

    private final Map<String, TarantoolSpaceMetadata> spaceMetadata = new HashMap<>();
    private final Map<String, Map<String, TarantoolIndexMetadata>> indexMetadata = new HashMap<>();

    public ProxyTarantoolMetadataContainer() {
    }

    @Override
    public Map<String, TarantoolSpaceMetadata> getSpaceMetadataByName() {
        return spaceMetadata;
    }

    @Override
    public Map<String, Map<String, TarantoolIndexMetadata>> getIndexMetadataBySpaceName() {
        return indexMetadata;
    }

    public void addSpace(TarantoolSpaceMetadata metadata) {
        spaceMetadata.put(metadata.getSpaceName(), metadata);
    }

    public void addIndexes(String spaceName, Map<String, TarantoolIndexMetadata> indexMetadataMap) {
        indexMetadata.put(spaceName, indexMetadataMap);
    }
}
