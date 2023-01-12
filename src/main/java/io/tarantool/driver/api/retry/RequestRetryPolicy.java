package io.tarantool.driver.api.retry;

import io.tarantool.driver.api.connection.ConnectionSelectionStrategy;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
        CompletableFuture.runAsync(() -> runAsyncOperation(operation, resultFuture, lastExceptionWrapper), executor)
            .exceptionally(ex -> { // we should complete final exception if something went wrong in runAsyncOperation
                resultFuture.completeExceptionally(ex);
                return null;
            });
        return resultFuture;
    }

    /**
     * Run operation in asynchronous way with retrying.
     * Operation can be run many times but final result will store in result future.
     * <p>
     * Each operation attempt is limited with a timeout returned by {@link #getRequestTimeout()}.
     * See {@link TarantoolRequestRetryPolicies.InfiniteRetryPolicy} for example of implementation.
     *
     * @param operation            supplier for the operation to perform. Must return a new operation instance
     * @param resultFuture         future that store final result
     * @param lastExceptionWrapper the last exception obtained in retrying chain
     * @param <T>                  operation result type
     */
    default <T> void runAsyncOperation(
        Supplier<CompletableFuture<T>> operation, CompletableFuture<T> resultFuture,
        AtomicReference<Throwable> lastExceptionWrapper) {
        // start async operation running
        CompletableFuture<T> operationFuture = operation.get();
        // start scheduled request timeout task
        // it never completes correctly only exceptionally
        CompletableFuture<T> requestTimeoutFuture = failAfterRequestTimeout(resultFuture);

        operationFuture.acceptEither(requestTimeoutFuture, resultFuture::complete)
            .exceptionally(ex -> { // if requestTimeout has been raised or operation return exception
                // if future was completed exceptionally in wrapOperation
                if (resultFuture.isDone()) {
                    return null;
                }

                while (ex instanceof ExecutionException || ex instanceof CompletionException) {
                    ex = ex.getCause();
                }
                // to provide it if retrying has been stopped without correct result
                lastExceptionWrapper.set(ex);

                if (this.canRetryRequest(ex)) {
                    // retry it after delay
                    ScheduledFuture<?> delayFuture =
                        TarantoolRequestRetryPolicies.getTimeoutScheduler().schedule(() -> {
                            runAsyncOperation(operation, resultFuture, lastExceptionWrapper);
                        }, getDelay(), TimeUnit.MILLISECONDS);
                    // optimization: stop delayed future if resultFuture has already done from outside
                    resultFuture.whenComplete((r, e) -> delayFuture.cancel(false));
                } else {
                    resultFuture.completeExceptionally(ex);
                }
                return null;
            }).exceptionally(ex -> { // if error has been happened in previous exceptionally section
                resultFuture.completeExceptionally(ex);
                return null;
            });
    }

    default <T> CompletableFuture<T> failAfterRequestTimeout(CompletableFuture<T> resultFuture) {
        long requestTimeout = getRequestTimeout();
        final CompletableFuture<T> future = new CompletableFuture<>();

        ScheduledFuture<Boolean> scheduledFuture = TarantoolRequestRetryPolicies.getTimeoutScheduler().schedule(() -> {
            final TimeoutException ex = new TimeoutException("Request timeout after " + requestTimeout);
            return future.completeExceptionally(ex);
        }, requestTimeout, TimeUnit.MILLISECONDS);
        // optimization: stop timeout future if resultFuture has already done from outside
        resultFuture.whenComplete((res, ex) -> scheduledFuture.cancel(false));

        return future;
    }
}
