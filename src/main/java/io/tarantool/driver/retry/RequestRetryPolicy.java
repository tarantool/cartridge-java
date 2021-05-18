package io.tarantool.driver.retry;

import io.tarantool.driver.ConnectionSelectionStrategy;
import io.tarantool.driver.exceptions.TarantoolTimeoutException;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Request retry policy contains an algorithm of deciding whether an exception is retriable and settings for
 * limiting the retry attempts
 *
 * @author Alexey Kuzin
 * @author Vladimir Rogach
 */
public interface RequestRetryPolicy {
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
        return TimeUnit.HOURS.toMillis(1);
    }

    /**
     * Wrap a generic operation taking an arbitrary number of arguments and returning a {@link CompletableFuture}.
     *
     * Each operation attempt is limited with a timeout returned by {@link #getRequestTimeout()}.
     * See {@link TarantoolRequestRetryPolicies.InfiniteRetryPolicy} for example of implementation.
     *
     * @param operation supplier for the operation to perform. Must return a new operation instance
     * @param executor  executor in which the retry callbacks will be scheduled
     * @param <T>       operation result type
     * @return {@link CompletableFuture} with the same type as the operation result type
     */
    default <T> CompletableFuture<T> wrapOperation(Supplier<CompletableFuture<T>> operation, Executor executor) {
        Assert.notNull(operation, "Operation must not be null");
        Assert.notNull(executor, "Executor must not be null");

        return CompletableFuture.supplyAsync(() -> {
            Throwable ex;
            do {
                CompletableFuture<T> f = operation.get();
                try {
                    return f.get(getRequestTimeout(), TimeUnit.MILLISECONDS);
                } catch (TimeoutException | InterruptedException e) {
                    ex = e;
                } catch (ExecutionException e) {
                    ex = e.getCause();
                }

            } while (this.canRetryRequest(ex));
            throw new CompletionException(ex);
        }, executor);
    }
}
