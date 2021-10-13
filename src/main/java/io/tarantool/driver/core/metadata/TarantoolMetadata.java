package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.utils.Assert;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for {@link TarantoolMetadataOperations} implementations
 *
 * @author Alexey Kuzin
 */
public class TarantoolMetadata implements TarantoolMetadataOperations {

    protected final Map<String, TarantoolSpaceMetadata> spaceMetadataByName = new ConcurrentHashMap<>();
    protected final Map<Integer, TarantoolSpaceMetadata> spaceMetadataById = new ConcurrentHashMap<>();
    protected final Map<String, Map<String, TarantoolIndexMetadata>> indexMetadataBySpaceName =
            new ConcurrentHashMap<>();
    protected final Map<Integer, Map<String, TarantoolIndexMetadata>> indexMetadataBySpaceId =
            new ConcurrentHashMap<>();
    private final Phaser initPhaser = new Phaser(0);
    private final AtomicBoolean needRefresh = new AtomicBoolean(true);
    private final TarantoolMetadataProvider metadataProvider;

    public TarantoolMetadata(TarantoolMetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    protected Map<String, TarantoolSpaceMetadata> getSpaceMetadata() {
        awaitInitLatch();
        return spaceMetadataByName;
    }

    protected Map<Integer, TarantoolSpaceMetadata> getSpaceMetadataById() {
        awaitInitLatch();
        return spaceMetadataById;
    }

    protected Map<String, Map<String, TarantoolIndexMetadata>> getIndexMetadata() {
        awaitInitLatch();
        return indexMetadataBySpaceName;
    }

    protected Map<Integer, Map<String, TarantoolIndexMetadata>> getIndexMetadataBySpaceId() {
        awaitInitLatch();
        return indexMetadataBySpaceId;
    }

    @Override
    public void scheduleRefresh() {
        needRefresh.set(true);
    }

    @Override
    public CompletableFuture<Void> refresh() throws TarantoolClientException {
        return populateMetadata().whenComplete((v, ex) -> {
            if (ex != null) {
                needRefresh.set(true);
            }
            initPhaser.arriveAndDeregister();
        });
    }

    private void awaitInitLatch() {
        if (initPhaser.getRegisteredParties() == 0 && needRefresh.compareAndSet(true, false)) {
            initPhaser.register();
            try {
                refresh().get();
            } catch (InterruptedException e) {
                throw new TarantoolClientException("Failed to refresh spaces and indexes metadata", e);
            } catch (ExecutionException e) {
                throw new TarantoolClientException("Failed to refresh spaces and indexes metadata", e.getCause());
            }
        } else {
            initPhaser.awaitAdvance(initPhaser.getPhase());
        }
    }

    private CompletableFuture<Void> populateMetadata() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            result = metadataProvider.getMetadata().thenAccept(container -> {
                spaceMetadataByName.clear();
                spaceMetadataById.clear();
                indexMetadataBySpaceName.clear();
                indexMetadataBySpaceId.clear();

                indexMetadataBySpaceName.putAll(container.getIndexMetadataBySpaceName());
                container.getSpaceMetadataByName().forEach((spaceName, spaceMetadata) -> {
                    spaceMetadataByName.put(spaceName, spaceMetadata);
                    spaceMetadataById.put(spaceMetadata.getSpaceId(), spaceMetadata);
                    Map<String, TarantoolIndexMetadata> indexesForSpace =
                            indexMetadataBySpaceName.get(spaceMetadata.getSpaceName());
                    if (indexesForSpace != null) {
                        indexMetadataBySpaceId.put(spaceMetadata.getSpaceId(), indexesForSpace);
                    }
                });
            });
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

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
}
