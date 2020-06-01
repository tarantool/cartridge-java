package io.tarantool.driver;

import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.space.TarantoolSpaceOperations;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Basic Tarantool client interface
 *
 * @author Alexey Kuzin
 */
public interface TarantoolClient extends Closeable {
    /**
     * Connect the client to the specified address
     * @param address valid host name or IP address of a Tarantool server
     * @return connected client
     */
    TarantoolClient connect(InetSocketAddress address);

    /**
     * Get the client connected status
     * @return true, if the client is connected
     */
    boolean isConnected();

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
     * Execute a function defined on Tarantool instance
     * TODO example function call
     * @param functionName
     * @param arguments
     * @param <T> the desired function call result type
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<T> call(String functionName, Object... arguments) throws TarantoolClientException;

    // TODO eval method

    /**
     * Get the Tarantool client config passed to this client
     * @return {@link TarantoolClientConfig} instance
     */
    TarantoolClientConfig getConfig();
}
