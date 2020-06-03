package io.tarantool.driver.metadata;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientException;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Basic Tarantool spaces and indexes metadata implementation for standalone server
 */
public class TarantoolMetadata implements TarantoolMetadataOperations {

    private static final int VSPACE_SPACE_ID = 281; // System space with all space descriptions (_vspace)
    private static final int VINDEX_SPACE_ID = 289; // System space with all index descriptions (_vindex)

    private final TarantoolClient client;
    private final TarantoolSpaceMetadataConverter spaceMetadataMapper;
    private final TarantoolIndexMetadataConverter indexMetadataMapper;

    private Map<Integer, TarantoolSpaceMetadata> spaceMetadataById = new ConcurrentHashMap<>();
    private Map<String, TarantoolSpaceMetadata> spaceMetadata = new ConcurrentHashMap<>();
    private Map<Integer, Map<String, TarantoolIndexMetadata>> indexMetadata = new ConcurrentHashMap<>();

    /**
     * Basic constructor.
     * @param client configured {@link TarantoolClient} instance
     */
    public TarantoolMetadata(TarantoolClient client) {
        this.client = client;
        this.spaceMetadataMapper = new TarantoolSpaceMetadataConverter(client.getConfig().getValueMapper());
        this.indexMetadataMapper = new TarantoolIndexMetadataConverter(client.getConfig().getValueMapper());
    }

    @Override
    public void refresh() throws TarantoolClientException {
        TarantoolIndexQuery query = new TarantoolIndexQuery(TarantoolIndexQuery.PRIMARY)
                .withIteratorType(TarantoolIteratorType.ITER_ALL);
        TarantoolSelectOptions options = new TarantoolSelectOptions.Builder().build();
        CompletableFuture<TarantoolResult<TarantoolSpaceMetadata>> spaces = client.space(VSPACE_SPACE_ID)
                .select(query, options, spaceMetadataMapper);
        spaces.thenApply((result) -> {
                        spaceMetadata.clear(); // clear the metadata only after the result fetching is successful
                        spaceMetadataById.clear();
                        return result;
                    })
                .thenAccept((result) -> result.stream()
                    .forEach((meta) -> {
                        spaceMetadata.put(meta.getSpaceName(), meta);
                        spaceMetadataById.put(meta.getSpaceId(), meta);
                    }));

        CompletableFuture<TarantoolResult<TarantoolIndexMetadata>> indexes = client.space(VINDEX_SPACE_ID)
                .select(query, options, indexMetadataMapper);
        indexes.thenApply((result) -> { indexMetadata.clear(); return result; })
                .thenAccept((result) -> result.stream()
                    .forEach((meta) -> {
                        indexMetadata.putIfAbsent(meta.getSpaceId(), new HashMap<>());
                        indexMetadata.get(meta.getSpaceId()).put(meta.getIndexName(), meta);
                    }));
        try {
            spaces.get();
            indexes.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceByName(String spaceName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        return Optional.ofNullable(spaceMetadata.get(spaceName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexForName(int spaceId, String indexName) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        Assert.hasText(indexName, "Index name must not be null or empty");

        Map<String, TarantoolIndexMetadata> metaMap = indexMetadata.get(spaceId);
        if (metaMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(metaMap.get(indexName));
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceById(int spaceId) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        return Optional.ofNullable(spaceMetadataById.get(spaceId));
    }
}
