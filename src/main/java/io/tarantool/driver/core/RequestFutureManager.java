package io.tarantool.driver.core;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class RequestFutureManager implements AutoCloseable {
    private final ScheduledExecutorService timeoutScheduler;
    private final TarantoolClientConfig config;
    private final Map<Long, TarantoolRequestMetadata> requestFutures = new ConcurrentHashMap<>();

    private static final Logger logger = LoggerFactory.getLogger(RequestFutureManager.class);

    /**
     * Basic constructor.
     * @param config tarantool client configuration
     * @param timeoutScheduler scheduled executor for handling request timeouts
     */
    public RequestFutureManager(TarantoolClientConfig config, ScheduledExecutorService timeoutScheduler) {
        this.config = config;
        this.timeoutScheduler = timeoutScheduler;
    }

    /**
     * Submit a request ID for tracking. Provides a {@link CompletableFuture} for tracking the request completion.
     * The request timeout is taken from the client configuration
     * @param request request to Tarantool server
     * @param resultMapper result message entity-to-object mapper
     * @param <T> target response body type
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    public <T> CompletableFuture<T> submitRequest(TarantoolRequest request, MessagePackValueMapper resultMapper) {
        return submitRequest(request, config.getRequestTimeout(), resultMapper);
    }

    /**
     * Submit a request ID for tracking. Provides a {@link CompletableFuture} for tracking the request completion.
     * The request timeout is taken from the client configuration
     * @param request request to Tarantool server
     * @param requestTimeout timeout after which the request will be automatically failed, milliseconds
     * @param resultMapper result message entity-to-object mapper
     * @param <T> target response body type
     * @return {@link CompletableFuture} that completes when a response is received from Tarantool server
     */
    public <T> CompletableFuture<T> submitRequest(TarantoolRequest request,
                                                  int requestTimeout,
                                                  MessagePackValueMapper resultMapper) {
        CompletableFuture<T> requestFuture = new CompletableFuture<>();
        long requestId = request.getHeader().getSync();
        requestFuture.whenComplete((r, e) -> {
            String message = "Request {} is completed {}";
            if (e != null) {
                logger.debug(message, requestId, "exceptionally");
            } else {
                logger.debug(message, requestId, "normally");
            }
            requestFutures.remove(requestId);
        });
        requestFutures.put(requestId, new TarantoolRequestMetadata(requestFuture, resultMapper));
        timeoutScheduler.schedule(() -> {
            if (!requestFuture.isDone()) {
                requestFuture.completeExceptionally(new TimeoutException(String.format(
                        "Failed to get response for request %d within %d ms", requestId, requestTimeout)));
            }
        }, requestTimeout, TimeUnit.MILLISECONDS);
        logger.debug("Submitted request {}", requestId);
        return requestFuture;
    }

    /**
     * Get a request me instance bound to the passed request ID
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
