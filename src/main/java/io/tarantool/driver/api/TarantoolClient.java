package io.tarantool.driver.api;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;

/**
 * Basic Tarantool client interface
 *
 * @author Alexey Kuzin
 */
public interface TarantoolClient extends AutoCloseable, TarantoolCallOperations, TarantoolEvalOperations {
    /**
     * Get the Tarantool client config passed to this client
     * @return {@link TarantoolClientConfig} instance
     */
    TarantoolClientConfig getConfig();

    /**
     * Get the Tarantool server version
     * @return {@link TarantoolVersion}
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolVersion getVersion() throws TarantoolClientException;

    /**
     * Provides CRUD and other operations for a Tarantool space
     * @param spaceName name of the space, must not be null or empty
     * @return Tarantool space operations interface
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolSpaceOperations space(String spaceName) throws TarantoolClientException;

    /**
     * Provides CRUD and other operations for a Tarantool space
     * @param spaceId ID of the space, must be greater than 0
     * @return Tarantool space operations implementation
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException;

    /**
     * Provides operations for Tarantool spaces and indexes metadata
     * @return Tarantool metadata operations implementation
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolMetadataOperations metadata() throws TarantoolClientException;

    /**
     * Get collection of connection listeners. Used for adding new listeners, removing listeners or examining
     * the collection
     *
     * @return connection listeners
     */
    TarantoolConnectionListeners getListeners();

    /**
     * Get the default factory for {@link TarantoolTuple} instances
     *
     * @return tuple factory instance
     */
    TarantoolTupleFactory getTarantoolTupleFactory();
}
