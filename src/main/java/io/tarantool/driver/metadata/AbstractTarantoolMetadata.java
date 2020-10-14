package io.tarantool.driver.metadata;

import io.tarantool.driver.exceptions.TarantoolClientException;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Base class for {@link TarantoolMetadataOperations} implementations
 *
 * @author Alexey Kuzin
 */
public abstract class AbstractTarantoolMetadata implements TarantoolMetadataOperations {

    protected final Map<String, TarantoolSpaceMetadata> spaceMetadata = new ConcurrentHashMap<>();
    protected final Map<Integer, TarantoolSpaceMetadata> spaceMetadataById = new ConcurrentHashMap<>();
    protected final Map<String, Map<String, TarantoolIndexMetadata>> indexMetadata = new ConcurrentHashMap<>();
    protected final Map<Integer, Map<String, TarantoolIndexMetadata>> indexMetadataBySpaceId =
            new ConcurrentHashMap<>();
    private final CountDownLatch initLatch = new CountDownLatch(1);

    protected Map<String, TarantoolSpaceMetadata> getSpaceMetadata() {
        awaitInitLatch();
        return spaceMetadata;
    }

    protected Map<Integer, TarantoolSpaceMetadata> getSpaceMetadataById() {
        awaitInitLatch();
        return spaceMetadataById;
    }

    protected Map<String, Map<String, TarantoolIndexMetadata>> getIndexMetadata() {
        awaitInitLatch();
        return indexMetadata;
    }

    protected Map<Integer, Map<String, TarantoolIndexMetadata>> getIndexMetadataBySpaceId() {
        awaitInitLatch();
        return indexMetadataBySpaceId;
    }

    @Override
    public CompletableFuture<Void> refresh() throws TarantoolClientException {
        return populateMetadata().whenComplete((v, ex) -> {
            if (initLatch.getCount() > 0) {
                initLatch.countDown();
            }
            if (ex != null) {
                throw new TarantoolClientException("Failed to refresh spaces and indexes metadata", ex);
            }
        });
    }

    protected abstract CompletableFuture<Void> populateMetadata();

    @Override
    public Optional<TarantoolSpaceMetadata> getSpaceByName(String spaceName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        return Optional.ofNullable(getSpaceMetadata().get(spaceName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexByName(int spaceId, String indexName) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        Assert.hasText(indexName, "Index name must not be null or empty");

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadataBySpaceId().get(spaceId);
        if (metaMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(metaMap.get(indexName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexByName(String spaceName, String indexName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        Assert.hasText(indexName, "Index name must not be null or empty");

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadata().get(spaceName);
        if (metaMap == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(metaMap.get(indexName));
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexById(String spaceName, int indexId) {
        Assert.hasText(spaceName, "Space name must not be null or empty");
        Assert.state(indexId >= 0, "Index ID must be greater than or equal 0");

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadata().get(spaceName);
        if (metaMap == null) {
            return Optional.empty();
        }

        return metaMap.values().stream().filter(i -> i.getIndexId() == indexId).findFirst();
    }

    @Override
    public Optional<TarantoolIndexMetadata> getIndexById(int spaceId, int indexId) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        Assert.state(indexId >= 0, "Index ID must be greater than or equal 0");

        Map<String, TarantoolIndexMetadata> metaMap = getIndexMetadataBySpaceId().get(spaceId);
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

    @Override
    public Optional<Map<String, TarantoolIndexMetadata>> getSpaceIndexes(int spaceId) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        return Optional.ofNullable(getIndexMetadataBySpaceId().get(spaceId));
    }

    @Override
    public Optional<Map<String, TarantoolIndexMetadata>> getSpaceIndexes(String spaceName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        return Optional.ofNullable(getIndexMetadata().get(spaceName));
    }

    private void awaitInitLatch() {
        if (initLatch.getCount() > 0) {
            refresh();
        }
        try {
            initLatch.await();
        } catch (InterruptedException e) {
            throw new TarantoolClientException(e);
        }
    }
}
