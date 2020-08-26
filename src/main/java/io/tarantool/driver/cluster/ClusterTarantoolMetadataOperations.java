package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.TarantoolResultMapperFactory;
import io.tarantool.driver.metadata.ClusterTarantoolSpaceMetadataContainer;
import io.tarantool.driver.metadata.ClusterTarantoolSpaceMetadataConverter;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Works with cluster space metadata
 *
 * @author Sergey Volgin
 */
public class ClusterTarantoolMetadataOperations implements TarantoolMetadataOperations {

    private final String getMetadataFunctionName;
    private TarantoolClient client;
    private TarantoolResultMapperFactory tarantoolResultMapperFactory;
    private ClusterTarantoolSpaceMetadataConverter metadataConverter;

    private Map<String, TarantoolSpaceMetadata> spaceMetadata = new ConcurrentHashMap<>();
    private Map<String, Map<String, TarantoolIndexMetadata>> indexMetadata = new ConcurrentHashMap<>();

    public ClusterTarantoolMetadataOperations(String getMetadataFunctionName,
                                              TarantoolClient client) {
        this.getMetadataFunctionName = getMetadataFunctionName;
        this.client = client;

        this.tarantoolResultMapperFactory = new TarantoolResultMapperFactory();
        this.metadataConverter = new ClusterTarantoolSpaceMetadataConverter(client.getConfig().getMessagePackMapper());
    }

    @Override
    public CompletableFuture<Void> refresh() throws TarantoolClientException {
        return client.call(getMetadataFunctionName,
                tarantoolResultMapperFactory.withSingleValueConverter(ClusterTarantoolSpaceMetadataContainer.class,
                        metadataConverter))
                .thenAccept(result -> {
                    spaceMetadata.clear();
                    indexMetadata.clear();
                    spaceMetadata.putAll(((ClusterTarantoolSpaceMetadataContainer) result.get(0)).getSpaceMetadata());
                    indexMetadata.putAll(((ClusterTarantoolSpaceMetadataContainer) result.get(0)).getIndexMetadata());
                });
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceByName(String spaceName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        return Optional.ofNullable(spaceMetadata.get(spaceName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexByName(String spaceName, String indexName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        Assert.hasText(indexName, "Index name must not be null or empty");

        TarantoolSpaceMetadata spaceMeta = spaceMetadata.get(spaceName);
        if (spaceMeta == null) {
            return Optional.empty();
        }

        Map<String, TarantoolIndexMetadata> metaMap = indexMetadata.get(spaceMeta.getSpaceName());
        if (metaMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(metaMap.get(indexName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexById(String spaceName, int indexId) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        Assert.state(indexId >= 0, "Index ID must be greater than or equal 0");

        TarantoolSpaceMetadata spaceMeta = spaceMetadata.get(spaceName);
        if (spaceMeta == null) {
            return Optional.empty();
        }

        Map<String, TarantoolIndexMetadata> metaMap = indexMetadata.get(spaceMeta.getSpaceName());
        if (metaMap == null) {
            return Optional.empty();
        }

        return metaMap.values().stream().filter(i -> i.getIndexId() == indexId).findFirst();
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexForName(int spaceId, String indexName) {
        throw new TarantoolClientException("Proxy client do not support work with space by ID");
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexForId(int spaceId, int indexId) {
        throw new TarantoolClientException("Proxy client do not support work with space by ID");
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceById(int spaceId) {
        throw new TarantoolClientException("Proxy client do not support work with space by ID");
    }
}
