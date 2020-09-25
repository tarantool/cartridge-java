package io.tarantool.driver.core;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.util.concurrent.CompletableFuture;

public interface TarantoolConnection extends AutoCloseable {
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
     * @param <T> result type
     * @return result future
     * @throws TarantoolProtocolException if the client is not connected or an error has occurred while
     * sending the request
     */
    <T> CompletableFuture<T> sendRequest(TarantoolRequest request, MessagePackValueMapper resultMapper)
            throws TarantoolProtocolException;

    /**
     * Add a listener which is invoked when the connection is broken from the server side (e.g. server closed
     * the connection or a network failure has occurred).
     * @param listener a {@link TarantoolConnectionFailureListener} instance
     */
    void addConnectionFailureListener(TarantoolConnectionFailureListener listener);
}
