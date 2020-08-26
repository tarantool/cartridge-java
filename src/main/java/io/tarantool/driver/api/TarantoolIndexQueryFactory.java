package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;

import java.util.Optional;

/**
 * A factory for index query used in select request and other requests to Tarantool server
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexQueryFactory {
    private TarantoolMetadataOperations metadataOperations;

    /**
     * Basic constructor.
     * @param metadataOperations a configured {@link TarantoolMetadataOperations} instance
     */
    public TarantoolIndexQueryFactory(TarantoolMetadataOperations metadataOperations) {
        this.metadataOperations = metadataOperations;
    }

    /**
     * Create a query for primary index
     * @return new {@link TarantoolIndexQuery} instance
     */
    public TarantoolIndexQuery primary() {
        return new TarantoolIndexQuery();
    }

    /**
     * Create a query for index by its name
     * @param spaceId ID of Tarantool space
     * @param indexName the index name
     * @return new {@link TarantoolIndexQuery} instance
     * @throws TarantoolClientException if failed to retrieve metadata from the Tarantool server
     */
    public TarantoolIndexQuery byName(int spaceId, String indexName) throws TarantoolClientException {
        Optional<TarantoolIndexMetadata> meta = metadataOperations.getIndexForName(spaceId, indexName);
        if (!meta.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceId, indexName);
        }
        return new TarantoolIndexQuery(meta.get().getIndexId());
    }

    /**
     * Create a query for index by its name
     *
     * @param indexName the index name
     * @param spaceName name of Tarantool space
     * @param operations a {@link TarantoolMetadataOperations} instance
     * @return new {@link TarantoolIndexQuery} instance
     * @throws TarantoolClientException if failed to retrieve metadata from the Tarantool cluster
     */
    public TarantoolIndexQuery byId(String indexName, String spaceName, TarantoolMetadataOperations operations)
            throws TarantoolClientException {
        Optional<TarantoolIndexMetadata> meta = operations.getIndexByName(spaceName, indexName);
        if (!meta.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceName, indexName);
        }
        return new TarantoolIndexQuery(meta.get().getIndexId());
    }
}
