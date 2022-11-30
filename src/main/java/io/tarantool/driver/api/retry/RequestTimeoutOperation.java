package io.tarantool.driver.api.retry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Supplier of scheduled request timeout tasks.
 * These tasks are never completed successfully, but only with a timeout exception.
 *
 * @author Artyom Dubinin
 */
public class RequestTimeoutOperation<T> implements Supplier<CompletableFuture<T>> {
    private final CompletableFuture<T> resultFuture;
    private final long requestTimeout;

    public RequestTimeoutOperation(CompletableFuture<T> resultFuture, long requestTimeout) {
        this.resultFuture = resultFuture;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public CompletableFuture<T> get() {
        final CompletableFuture<T> future = new CompletableFuture<>();
        ScheduledFuture<Boolean> scheduledFuture = TarantoolRequestRetryPolicies.getTimeoutScheduler().schedule(() -> {
            final TimeoutException ex = new TimeoutException("Request timeout after " + requestTimeout + " ms");
            return future.completeExceptionally(ex);
        }, requestTimeout, TimeUnit.MILLISECONDS);
        // optimization: stop timeout future if resultFuture is already completed externally
        resultFuture.whenComplete((res, ex) -> scheduledFuture.cancel(false));
        return future;
    }
}
