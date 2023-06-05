package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains spaces and indexes metadata information retrieved from the system spaces
 *
 * @author Alexey Kuzin
 */
public class SpacesTarantoolMetadataContainer implements TarantoolMetadataContainer {

    private final Map<String, TarantoolSpaceMetadata> spaceMetadataByName = new HashMap<>();
    private final Map<Integer, TarantoolSpaceMetadata> spaceMetadataById = new HashMap<>();
    private final Map<String, Map<Object, TarantoolIndexMetadata>> indexMetadataBySpaceName = new HashMap<>();

    public SpacesTarantoolMetadataContainer(
        TarantoolResult<TarantoolSpaceMetadata> spacesCollection,
        TarantoolResult<TarantoolIndexMetadata> indexesCollection) {
        spacesCollection.forEach(meta -> {
            spaceMetadataByName.put(meta.getSpaceName(), meta);
            spaceMetadataById.put(meta.getSpaceId(), meta);
        });

        indexesCollection.forEach(meta -> {
            String spaceName = spaceMetadataById.get(meta.getSpaceId()).getSpaceName();
            indexMetadataBySpaceName.putIfAbsent(spaceName, new HashMap<>());
            indexMetadataBySpaceName.get(spaceName).put(meta.getIndexName(), meta);
        });
    }

    @Override
    public Map<String, TarantoolSpaceMetadata> getSpaceMetadataByName() {
        return spaceMetadataByName;
    }

    @Override
    public Map<String, Map<Object, TarantoolIndexMetadata>> getIndexMetadataBySpaceName() {
        return indexMetadataBySpaceName;
    }
}
