package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.msgpack.value.Value;

/**
 * Keeps track of submitted requests, finishing them by timeout and allowing asynchronous request processing
 *
 * @author Alexey Kuzin
 */
public class RequestFutureManager implements AutoCloseable {
    private final ScheduledExecutorService timeoutScheduler;
    private final TarantoolClientConfig config;
    private final Map<Long, TarantoolRequestMetadata> requestFutures = new ConcurrentHashMap<>();

    /**
     * Basic constructor.
     *
     * @param config           tarantool client configuration
     * @param timeoutScheduler scheduled executor for handling request timeouts
     */
    public RequestFutureManager(TarantoolClientConfig config, ScheduledExecutorService timeoutScheduler) {
        this.config = config;
        this.timeoutScheduler = timeoutScheduler;
    }

    /**
     * Submit a request ID for tracking. Provides a {@link CompletableFuture} for tracking the request completion.
     * The request timeout is taken from the client configuration
     *
     * @param request      request to Tarantool server
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    public CompletableFuture<Value> submitRequest(TarantoolRequest request) {
        return submitRequest(request, config.getRequestTimeout());
    }

    /**
     * Submit a request ID for tracking. Provides a {@link CompletableFuture} for tracking the request completion.
     * The request timeout is taken from the client configuration
     *
     * @param request        request to Tarantool server
     * @param requestTimeout timeout after which the request will be automatically failed, milliseconds
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    public CompletableFuture<Value> submitRequest(
        TarantoolRequest request,
        int requestTimeout) {
        CompletableFuture<Value> requestFuture = new CompletableFuture<>();
        long requestId = request.getHeader().getSync();
        requestFuture.whenComplete((r, e) -> requestFutures.remove(requestId));
        requestFutures.put(requestId, new TarantoolRequestMetadata(requestFuture));
        timeoutScheduler.schedule(() -> {
            if (!requestFuture.isDone()) {
                requestFuture.completeExceptionally(new TimeoutException(String.format(
                    "Failed to get response for request id: %d within %d ms", requestId, requestTimeout)));
            }
        }, requestTimeout, TimeUnit.MILLISECONDS);
        return requestFuture;
    }

    /**
     * Get a request me instance bound to the passed request ID
     *
     * @param requestId ID of a request to Tarantool server (sync ID)
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    public TarantoolRequestMetadata getRequest(Long requestId) {
        return requestFutures.get(requestId);
    }

    @Override
    public void close() {
        requestFutures.values().forEach(f -> f.getFuture().join());
    }
}
