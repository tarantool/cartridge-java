package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Keeps track of submitted requests, finishing them by timeout and allowing asynchronous request processing
 *
 * @author Alexey Kuzin
 */
public class RequestFutureManagerImpl implements RequestFutureManager {

    private final ScheduledExecutorService timeoutScheduler;
    private final TarantoolClientConfig config;
    private final Map<Long, TarantoolRequestMetadata> requestFutures = new ConcurrentHashMap<>();

    /**
     * Basic constructor.
     *
     * @param config           tarantool client configuration
     * @param timeoutScheduler scheduled executor for handling request timeouts
     */
    public RequestFutureManagerImpl(TarantoolClientConfig config, ScheduledExecutorService timeoutScheduler) {
        this.config = config;
        this.timeoutScheduler = timeoutScheduler;
    }

    @Override
    public <T> CompletableFuture<T> submitRequest(TarantoolRequest request, MessagePackValueMapper resultMapper) {
        return submitRequest(request, config.getRequestTimeout(), resultMapper);
    }

    @Override
    public <T> CompletableFuture<T> submitRequest(TarantoolRequest request,
                                                  int requestTimeout,
                                                  MessagePackValueMapper resultMapper) {
        CompletableFuture<T> requestFuture = new CompletableFuture<>();
        long requestId = request.getHeader().getSync();
        requestFuture.whenComplete((r, e) -> requestFutures.remove(requestId));
        requestFutures.put(requestId, new TarantoolRequestMetadata(requestFuture, resultMapper));
        timeoutScheduler.schedule(() -> {
            if (!requestFuture.isDone()) {
                requestFuture.completeExceptionally(new TimeoutException(String.format(
                        "Failed to get response for request id: %d within %d ms", requestId, requestTimeout)));
            }
        }, requestTimeout, TimeUnit.MILLISECONDS);
        return requestFuture;
    }

    @Override
    public TarantoolRequestMetadata getRequest(Long requestId) {
        return requestFutures.get(requestId);
    }

    @Override
    public void close() {
        requestFutures.values().forEach(f -> f.getFuture().join());
    }
}
