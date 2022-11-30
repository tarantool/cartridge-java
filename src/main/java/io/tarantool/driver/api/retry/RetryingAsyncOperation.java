package io.tarantool.driver.api.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Wrapper to run operation in an asynchronous way with retries.
 * Operation can be run many times but the final result will be stored in a result future.
 * <p>
 * Each operation attempt is limited with a timeout returned by {@link RequestRetryPolicy#getRequestTimeout()}.
 * See {@link TarantoolRequestRetryPolicies.InfiniteRetryPolicy} for an implementation example.
 *
 * @author Artyom Dubinin
 */
public class RetryingAsyncOperation<T> implements Runnable {
    protected final Supplier<CompletableFuture<T>> operation;
    protected final CompletableFuture<T> resultFuture;
    protected final AtomicReference<Throwable> lastExceptionWrapper;
    protected final RequestTimeoutOperation<T> requestTimeoutOperation;
    protected final RequestRetryPolicy policy;
    private final Logger log = LoggerFactory.getLogger(RetryingAsyncOperation.class);

    public RetryingAsyncOperation(
        RequestRetryPolicy policy,
        Supplier<CompletableFuture<T>> operation, CompletableFuture<T> resultFuture,
        AtomicReference<Throwable> lastExceptionWrapper) {
        this.operation = operation;
        this.resultFuture = resultFuture;
        this.lastExceptionWrapper = lastExceptionWrapper;
        this.requestTimeoutOperation = new RequestTimeoutOperation<>(resultFuture, policy.getRequestTimeout());
        this.policy = policy;
    }

    @Override
    public void run() {
        // start async operation running
        CompletableFuture<T> operationFuture = operation.get();
        // start scheduled request timeout task
        // the timeout task can only be completed with an exception or canceled
        CompletableFuture<T> requestTimeoutFuture = requestTimeoutOperation.get();

        operationFuture.acceptEither(requestTimeoutFuture, resultFuture::complete)
            .whenComplete((futureResult, ex) -> { // if requestTimeout has been raised or operation return exception
                try {
                    // if future has been completed in wrapOperation
                    // for example:
                    // if you had global timeout above async retrying operation,
                    // the result future could be completed by this timeout
                    if (resultFuture.isDone()) {
                        return;
                    }

                    while (ex instanceof ExecutionException || ex instanceof CompletionException) {
                        ex = ex.getCause();
                    }
                    // to provide it if retrying has been stopped without correct result
                    lastExceptionWrapper.set(ex);
                    // to be able to track trace of all errors
                    log.debug("Retrying exception - " + ex.getMessage());

                    if (policy.canRetryRequest(ex)) {
                        // retry it after delay
                        ScheduledFuture<?> delayFuture = TarantoolRequestRetryPolicies.getTimeoutScheduler()
                            .schedule(this, policy.getDelay(), TimeUnit.MILLISECONDS);
                        // optimization: stop delayed future if resultFuture has already done from outside
                        resultFuture.whenComplete((r, e) -> delayFuture.cancel(false));
                    } else {
                        resultFuture.completeExceptionally(policy.getPolicyException(ex));
                    }
                } catch (Exception internalException) {
                    resultFuture.completeExceptionally(internalException);
                }
            });
    }
}
