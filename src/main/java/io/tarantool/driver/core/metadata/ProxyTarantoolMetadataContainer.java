package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains spaces and indexes metadata information retrieved from a call to a stored function
 *
 * @author Sergey Volgin
 */
public class ProxyTarantoolMetadataContainer implements TarantoolMetadataContainer {

    private final Map<String, TarantoolSpaceMetadata> spaceMetadata = new HashMap<>();
    private final Map<String, Map<Object, TarantoolIndexMetadata>> indexMetadata = new HashMap<>();

    public ProxyTarantoolMetadataContainer() {
    }

    @Override
    public Map<String, TarantoolSpaceMetadata> getSpaceMetadataByName() {
        return spaceMetadata;
    }

    @Override
    public Map<String, Map<Object, TarantoolIndexMetadata>> getIndexMetadataBySpaceName() {
        return indexMetadata;
    }

    public void addSpace(TarantoolSpaceMetadata metadata) {
        spaceMetadata.put(metadata.getSpaceName(), metadata);
    }

    public void addIndexes(String spaceName, Map<Object, TarantoolIndexMetadata> indexMetadataMap) {
        indexMetadata.put(spaceName, indexMetadataMap);
    }
}
