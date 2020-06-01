package io.tarantool.driver.core;

import io.tarantool.driver.TarantoolClientConfig;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Keeps track of submitted requests, finishing them by timeout and allowing asynchronous request processing
 *
 * @author Alexey Kuzin
 */
public class RequestFutureManager {
    private Map<Long, CompletableFuture> requestFutures;
    private ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor();
    private TarantoolClientConfig config;

    public RequestFutureManager(TarantoolClientConfig config) {
        this.config = config;
        this.requestFutures = new ConcurrentHashMap<>();
    }

    /**
     * Submit a request ID for tracking. Provides a {@link CompletableFuture} for tracking the request completion.
     * @param requestId ID of a request to Tarantool server (sync ID)
     * @param requestTimeout timeout after
     * @param <T> target response body type
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    public <T> CompletableFuture<T> addRequestFuture(Long requestId, int requestTimeout) {
        CompletableFuture<T> requestFuture = new CompletableFuture<>();
        requestFuture.whenComplete((r, e) -> requestFutures.remove(requestId));
        requestFutures.put(requestId, requestFuture);
        timeoutScheduler.schedule(() -> {
            if (!requestFuture.isDone())
                requestFuture.completeExceptionally(new TimeoutException(
                        String.format("Failed to get response for request %d within %d ms", requestId, requestTimeout)));
        }, requestTimeout, TimeUnit.MILLISECONDS);
        return requestFuture;
    }

    /**
     * A shorthand for {@link #addRequestFuture(Long, int)}, where the request timeout is taken from the client configuration
     * @param requestId ID of a request to Tarantool server (sync ID)
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    public CompletableFuture addRequestFuture(Long requestId) {
        return addRequestFuture(requestId, config.getRequestTimeout());
    }

    /**
     * Get a {@link CompletableFuture} instance bound to the passed request ID
     * @param requestId ID of a request to Tarantool server (sync ID)
     * @param <T> target response body type
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> getRequestFuture(Long requestId) {
        return requestFutures.get(requestId);
    }
}
