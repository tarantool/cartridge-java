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
    private final TarantoolResultMapperFactory resultMapperFactory;
    private final CRUDTarantoolSpaceMetadataConverter metadataConverter;

    private final Map<String, TarantoolSpaceMetadata> spaceMetadata = new ConcurrentHashMap<>();
    private final Map<Integer, TarantoolSpaceMetadata> spaceMetadataById = new ConcurrentHashMap<>();
    private final Map<String, Map<String, TarantoolIndexMetadata>> indexMetadata = new ConcurrentHashMap<>();

    private final CountDownLatch initLatch = new CountDownLatch(1);

    public CRUDTarantoolMetadata(String getMetadataFunctionName,
                                 TarantoolClient client) {
        this.getMetadataFunctionName = getMetadataFunctionName;
        this.client = client;
        this.resultMapperFactory = new TarantoolResultMapperFactory();
        this.metadataConverter = new CRUDTarantoolSpaceMetadataConverter(client.getConfig().getMessagePackMapper());
    }

    @Override
    public CompletableFuture<Void> refresh() throws TarantoolClientException {

        CompletableFuture<List<CRUDTarantoolSpaceMetadataContainer>> callResult =
                client.call(getMetadataFunctionName,
                        resultMapperFactory.withProxyConverter(metadataConverter,
                                CRUDTarantoolSpaceMetadataContainer.class));

        return callResult.thenAccept(result -> {
            spaceMetadata.clear();
            spaceMetadataById.clear();
            indexMetadata.clear();
            result.forEach(sm -> {
                spaceMetadata.putAll(sm.getSpaceMetadata());
                sm.getSpaceMetadata().forEach((key, val) -> spaceMetadataById.put(val.getSpaceId(), val));
            });
            result.forEach(sm -> indexMetadata.putAll(sm.getIndexMetadata()));

        }).whenComplete((v, ex) -> {
            if (initLatch.getCount() > 0) {
                initLatch.countDown();
            }
            if (ex != null) {
                throw new TarantoolClientException("CRUD client space metadata refresh error.", ex);
            }
        });
    }

    public Map<String, TarantoolSpaceMetadata> getSpaceMetadata() {
        awaitInitLatch();
        return spaceMetadata;
    }

    private Map<Integer, TarantoolSpaceMetadata> getSpaceMetadataById() {
        awaitInitLatch();
        return spaceMetadataById;
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
    public Optional<TarantoolIndexMetadata> getIndexByName(int spaceId, String indexName) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        Assert.hasText(indexName, "Index name must not be null or empty");

        TarantoolSpaceMetadata spaceMeta = getSpaceMetadataById().get(spaceId);
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
    public Optional<TarantoolIndexMetadata> getIndexById(int spaceId, int indexId) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        Assert.state(indexId >= 0, "Index ID must be greater than or equal 0");

        TarantoolSpaceMetadata spaceMeta = getSpaceMetadataById().get(spaceId);
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
    public Optional<TarantoolSpaceMetadata> getSpaceById(int spaceId) {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");
        return Optional.ofNullable(getSpaceMetadataById().get(spaceId));
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
