package io.tarantool.driver.core;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.util.concurrent.CompletableFuture;

public interface RequestFutureManager extends AutoCloseable {

    /**
     * Submit a request ID for tracking. Provides a {@link CompletableFuture} for tracking the request completion.
     * The request timeout is taken from the client configuration
     *
     * @param request      request to Tarantool server
     * @param resultMapper result message entity-to-object mapper
     * @param <T>          target response body type
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    <T> CompletableFuture<T> submitRequest(TarantoolRequest request, MessagePackValueMapper resultMapper);

    /**
     * Submit a request ID for tracking. Provides a {@link CompletableFuture} for tracking the request completion.
     * The request timeout is taken from the client configuration
     *
     * @param request        request to Tarantool server
     * @param requestTimeout timeout after which the request will be automatically failed, milliseconds
     * @param resultMapper   result message entity-to-object mapper
     * @param <T>            target response body type
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    <T> CompletableFuture<T> submitRequest(TarantoolRequest request,
                                           int requestTimeout,
                                           MessagePackValueMapper resultMapper);

    /**
     * Get a request me instance bound to the passed request ID
     *
     * @param requestId ID of a request to Tarantool server (sync ID)
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    TarantoolRequestMetadata getRequest(Long requestId);

    void close();
}
