package io.tarantool.driver.proxy;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.TarantoolResultMapperFactory;
import io.tarantool.driver.metadata.CRUDTarantoolSpaceMetadataContainer;
import io.tarantool.driver.metadata.CRUDTarantoolSpaceMetadataConverter;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Works with CRUD client metadata
 *
 * @author Sergey Volgin
 */
public class CRUDTarantoolMetadata implements TarantoolMetadataOperations {

    private final String getMetadataFunctionName;
    private final TarantoolClient client;
    private final TarantoolResultMapperFactory tarantoolResultMapperFactory;
    private final CRUDTarantoolSpaceMetadataConverter metadataConverter;

    private final Map<String, TarantoolSpaceMetadata> spaceMetadata = new ConcurrentHashMap<>();
    private final Map<String, Map<String, TarantoolIndexMetadata>> indexMetadata = new ConcurrentHashMap<>();

    private final CountDownLatch initLatch = new CountDownLatch(1);

    public CRUDTarantoolMetadata(String getMetadataFunctionName,
                                 TarantoolClient client) {
        this.getMetadataFunctionName = getMetadataFunctionName;
        this.client = client;

        this.tarantoolResultMapperFactory = new TarantoolResultMapperFactory();
        this.metadataConverter = new CRUDTarantoolSpaceMetadataConverter(client.getConfig().getMessagePackMapper());

        client.getListeners().add(connection -> {
            try {
                return refresh().thenApply(v -> connection);
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> refresh() throws TarantoolClientException {

        CompletableFuture<List<CRUDTarantoolSpaceMetadataContainer>> callResult =
                client.call(getMetadataFunctionName,
                        tarantoolResultMapperFactory
                                .withSingleValueConverter(CRUDTarantoolSpaceMetadataContainer.class,
                                        metadataConverter));

        return callResult.thenAccept(result -> {
                    spaceMetadata.clear();
                    indexMetadata.clear();
                    spaceMetadata.putAll(result.get(0).getSpaceMetadata());
                    indexMetadata.putAll(result.get(0).getIndexMetadata());
                }).thenApply(v -> {
                    if (initLatch.getCount() > 0) {
                        initLatch.countDown();
                    }
                    return v;
                });
    }

    public Map<String, TarantoolSpaceMetadata> getSpaceMetadata() {
        awaitInitLatch();
        return spaceMetadata;
    }

    public Map<String, Map<String, TarantoolIndexMetadata>> getIndexMetadata() {
        awaitInitLatch();
        return indexMetadata;
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceByName(String spaceName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        return Optional.ofNullable(getSpaceMetadata().get(spaceName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexByName(String spaceName, String indexName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        Assert.hasText(indexName, "Index name must not be null or empty");

        TarantoolSpaceMetadata spaceMeta = getSpaceMetadata().get(spaceName);
        if (spaceMeta == null) {
            return Optional.empty();
        }

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadata().get(spaceMeta.getSpaceName());
        if (metaMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(metaMap.get(indexName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexById(String spaceName, int indexId) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        Assert.state(indexId >= 0, "Index ID must be greater than or equal 0");

        TarantoolSpaceMetadata spaceMeta = getSpaceMetadata().get(spaceName);
        if (spaceMeta == null) {
            return Optional.empty();
        }

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadata().get(spaceMeta.getSpaceName());
        if (metaMap == null) {
            return Optional.empty();
        }

        return metaMap.values().stream().filter(i -> i.getIndexId() == indexId).findFirst();
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexForName(int spaceId, String indexName) {
        throw new TarantoolClientException("CRUD client do not support work with space by ID");
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexForId(int spaceId, int indexId) {
        throw new TarantoolClientException("CRUD client do not support work with space by ID");
    }

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceById(int spaceId) {
        throw new TarantoolClientException("CRUD client do not support work with space by ID");
    }

    private void awaitInitLatch() {
        try {
            initLatch.await();
        } catch (InterruptedException e) {
            throw new TarantoolClientException(e);
        }
    }
}
