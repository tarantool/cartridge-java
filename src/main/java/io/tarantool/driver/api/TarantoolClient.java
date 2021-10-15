package io.tarantool.driver.api;

import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.api.connection.TarantoolConnectionListeners;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;

/**
 * Basic Tarantool client interface
 *
 * @author Alexey Kuzin
 */
public interface TarantoolClient<T extends Packable, R extends Collection<T>>
        extends AutoCloseable, TarantoolCallOperations, TarantoolEvalOperations {
    /**
     * Provides implementation of retrieving the metadata for spaces and instances from Tarantool servers
     *
     * @return metadata provider instance
     */
    TarantoolMetadataProvider metadataProvider();

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
    TarantoolSpaceOperations<T, R> space(String spaceName) throws TarantoolClientException;

    /**
     * Provides CRUD and other operations for a Tarantool space
     * @param spaceId ID of the space, must be greater than 0
     * @return Tarantool space operations implementation
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolSpaceOperations<T, R> space(int spaceId) throws TarantoolClientException;

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
    TarantoolConnectionListeners getConnectionListeners();
}
