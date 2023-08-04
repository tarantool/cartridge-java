package io.tarantool.driver.api.retry;

import io.tarantool.driver.api.connection.ConnectionSelectionStrategy;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Request retry policy contains an algorithm of deciding whether an exception is retryable and settings for
 * limiting the retry attempts
 *
 * @author Alexey Kuzin
 * @author Vladimir Rogach
 * @author Artyom Dubinin
 */
@FunctionalInterface
public interface RequestRetryPolicy {

    int DEFAULT_DELAY = 500;
    long DEFAULT_REQUEST_TIMEOUT = TimeUnit.HOURS.toMillis(1);

    /**
     * A callback called when the request ends with an exception. Should return {@code true} if and only if the request
     * may be performed again (e.g. it is a timeout exception and it indicates only that the current server is
     * overloaded). This may depend not only on the exception type, but also on the other conditions like the allowed
     * number of retries or the maximum request execution time.
     * <p>
     * Effective use of the retry policies may be achieved by combining them with multiple server connections and a
     * {@link ConnectionSelectionStrategy}.
     *
     * @param throwable exception a request failed with
     * @return true if the request may be retried
     */
    boolean canRetryRequest(Throwable throwable);

    /**
     * Get timeout value for one retry attempt. The default value is 1 hour.
     *
     * @return timeout value (ms), should be greater or equal to 0
     */
    default long getRequestTimeout() {
        return DEFAULT_REQUEST_TIMEOUT;
    }

    /**
     * Delay that is used to wait before start attempt again.
     *
     * @return get delay between retrying attempts
     */
    default long getDelay() {
        return DEFAULT_DELAY;
    }

    /**
     * Get an exception stating why the policy cannot repeat the request
     *
     * @param ex exception that will be wrapped
     * @return reason of stopping retrying
     */
    default Throwable getPolicyException(Throwable ex) {
        return ex;
    }

    /**
     * Wrap a generic operation taking an arbitrary number of arguments and returning a {@link CompletableFuture}.
     * <p>
     *
     * @param operation supplier for the operation to perform. Must return a new operation instance
     * @param executor  executor in which the retry callbacks will be scheduled
     * @param <T>       operation result type
     * @return {@link CompletableFuture} with the same type as the operation result type
     */
    default <T> CompletableFuture<T> wrapOperation(Supplier<CompletableFuture<T>> operation, Executor executor) {
        Assert.notNull(operation, "Operation must not be null");
        Assert.notNull(executor, "Executor must not be null");

        // because we have asynchronous logic in completion stage chain
        // we should have sharing answer state for final result
        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        // to provide it if retrying has been stopped without correct result
        AtomicReference<Throwable> lastExceptionWrapper = new AtomicReference<>();
        CompletableFuture.runAsync(
                new RetryingAsyncOperation<>(this, operation, resultFuture, lastExceptionWrapper),
                executor
            )
            .exceptionally(ex -> {
                resultFuture.completeExceptionally(ex);
                return null;
            });
        return resultFuture;
    }
}
