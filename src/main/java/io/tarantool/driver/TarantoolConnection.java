package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.util.concurrent.CompletableFuture;

public interface TarantoolConnection extends AutoCloseable {
    /**
     * Connect to the configured Tarantool server
     * @return future holding this connection object, which completes once the authorization is complete
     * @throws TarantoolClientException
     */
    CompletableFuture<TarantoolConnection> connect() throws TarantoolClientException;

    /**
     * Get the Tarantool server version
     * @return {@link TarantoolVersion}
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolVersion getVersion() throws TarantoolClientException;

    /**
     * Get the connection status
     * @return true, if the connection is alive
     */
    boolean isConnected();

    /**
     * Send a prepared request to the Tarantool server and flush the buffer
     * @param request the request
     * @param resultMapper the mapper for response body
     * @return result future
     * @throws TarantoolProtocolException
     */
    <T> CompletableFuture<T> sendRequest(TarantoolRequest request, MessagePackValueMapper resultMapper)
            throws TarantoolProtocolException;
}
