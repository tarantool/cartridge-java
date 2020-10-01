package io.tarantool.driver.metadata;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
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
import java.util.concurrent.CountDownLatch;

/**
 * Basic Tarantool spaces and indexes metadata implementation for standalone server
 */
public class TarantoolMetadata implements TarantoolMetadataOperations {

    private static final int VSPACE_SPACE_ID = 281; // System space with all space descriptions (_vspace)
    private static final int VINDEX_SPACE_ID = 289; // System space with all index descriptions (_vindex)

    private final TarantoolSpaceMetadataConverter spaceMetadataMapper;
    private final TarantoolIndexMetadataConverter indexMetadataMapper;
    private final TarantoolClientConfig config;
    private final TarantoolConnectionManager connectionManager;
    private final CountDownLatch initLatch = new CountDownLatch(1);

    private final Map<Integer, TarantoolSpaceMetadata> spaceMetadataById = new ConcurrentHashMap<>();
    private final Map<String, TarantoolSpaceMetadata> spaceMetadata = new ConcurrentHashMap<>();
    private final Map<Integer, Map<String, TarantoolIndexMetadata>> indexMetadata = new ConcurrentHashMap<>();

    /**
     * Basic constructor.
     * @param config client configuration
     * @param connectionManager configured {@link TarantoolConnectionManager} instance
     */
    public TarantoolMetadata(TarantoolClientConfig config, TarantoolConnectionManager connectionManager) {
        this.spaceMetadataMapper = new TarantoolSpaceMetadataConverter(config.getMessagePackMapper());
        this.indexMetadataMapper = new TarantoolIndexMetadataConverter(config.getMessagePackMapper());
        this.config = config;
        this.connectionManager = connectionManager;
        refresh();
    }

    private TarantoolSpaceOperations space(int spaceId) {
        return new TarantoolSpace(spaceId, config, connectionManager, this);
    }

    @Override
    public CompletableFuture<Void> refresh() throws TarantoolClientException {
        TarantoolIndexQuery query = new TarantoolIndexQuery(TarantoolIndexQuery.PRIMARY)
                .withIteratorType(TarantoolIteratorType.ITER_ALL);
        TarantoolSelectOptions options = new TarantoolSelectOptions.Builder().build();
        CompletableFuture<TarantoolResult<TarantoolSpaceMetadata>> spaces = space(VSPACE_SPACE_ID)
                .select(query, options, spaceMetadataMapper);
        spaces.thenApply(result -> {
                        spaceMetadata.clear(); // clear the metadata only after the result fetching is successful
                        spaceMetadataById.clear();
                        return result;
                    })
                .thenAccept(result -> result.stream()
                    .forEach(meta -> {
                        spaceMetadata.put(meta.getSpaceName(), meta);
                        spaceMetadataById.put(meta.getSpaceId(), meta);
                    }));

        CompletableFuture<TarantoolResult<TarantoolIndexMetadata>> indexes = space(VINDEX_SPACE_ID)
                .select(query, options, indexMetadataMapper);
        indexes.thenApply(result -> {
                    indexMetadata.clear();
                    return result;
                })
                .thenAccept(result -> result.stream()
                    .forEach(meta -> {
                        indexMetadata.putIfAbsent(meta.getSpaceId(), new HashMap<>());
                        indexMetadata.get(meta.getSpaceId()).put(meta.getIndexName(), meta);
                    }));

        return CompletableFuture.allOf(spaces, indexes).whenComplete((v, ex) -> {
            if (initLatch.getCount() > 0) {
                initLatch.countDown();
            }
        });
    }

    private Map<Integer, TarantoolSpaceMetadata> getSpaceMetadataById() {
        try {
            initLatch.await();
        } catch (InterruptedException e) {
            throw new TarantoolClientException(e);
        }
        return spaceMetadataById;
    }

    private Map<String, TarantoolSpaceMetadata> getSpaceMetadata() {
        try {
            initLatch.await();
        } catch (InterruptedException e) {
            throw new TarantoolClientException(e);
        }
        return spaceMetadata;
    }

    private Map<Integer, Map<String, TarantoolIndexMetadata>> getIndexMetadata() {
        try {
            initLatch.await();
        } catch (InterruptedException e) {
            throw new TarantoolClientException(e);
        }
        return indexMetadata;
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceByName(String spaceName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        return Optional.ofNullable(getSpaceMetadata().get(spaceName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexForName(int spaceId, String indexName) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        Assert.hasText(indexName, "Index name must not be null or empty");

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadata().get(spaceId);
        if (metaMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(metaMap.get(indexName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexForId(int spaceId, int indexId) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        Assert.state(indexId >= 0, "Index ID must be greater than or equal 0");

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadata().get(spaceId);
        if (metaMap == null) {
            return Optional.empty();
        }

        return metaMap.values().stream().filter(i -> i.getIndexId() == indexId).findFirst();
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceById(int spaceId) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        return Optional.ofNullable(getSpaceMetadataById().get(spaceId));
    }
}
