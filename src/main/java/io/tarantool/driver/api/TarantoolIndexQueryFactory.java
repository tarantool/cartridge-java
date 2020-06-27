package io.tarantool.driver.api;

import io.tarantool.driver.TarantoolClientException;
import io.tarantool.driver.TarantoolConnection;
import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;

import java.util.Optional;

/**
 * A factory for index query used in select request and other requests to Tarantool server
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexQueryFactory {
    private TarantoolConnection connection;

    /**
     * Basic constructor.
     * @param connection a connected {@link TarantoolConnection} instance
     */
    public TarantoolIndexQueryFactory(TarantoolConnection connection) {
        this.connection = connection;
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
        Optional<TarantoolIndexMetadata> meta = connection.metadata().getIndexForName(spaceId, indexName);
        if (!meta.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceId, indexName);
        }
        return new TarantoolIndexQuery(meta.get().getIndexId());
    }
}
