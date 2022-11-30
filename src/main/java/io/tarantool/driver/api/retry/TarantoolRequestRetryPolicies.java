package io.tarantool.driver.api.retry;

import io.tarantool.driver.core.TarantoolDaemonThreadFactory;
import io.tarantool.driver.exceptions.TarantoolAttemptsLimitException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolConnectionException;
import io.tarantool.driver.exceptions.TarantoolInternalNetworkException;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import io.tarantool.driver.exceptions.TarantoolTimeoutException;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Class-container for built-in request retry policies
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 * @author Oleg Kuznetsov
 */
public final class TarantoolRequestRetryPolicies {

    private static final ScheduledExecutorService timeoutScheduler =
        Executors.newSingleThreadScheduledExecutor(new TarantoolDaemonThreadFactory("tarantool-retry-timeout"));

    public static final Predicate<Throwable> retryAll = t -> true;
    public static final Predicate<Throwable> retryNone = t -> false;
    public static final long DEFAULT_ONE_HOUR_TIMEOUT = TimeUnit.HOURS.toMillis(1); //ms

    /**
     * Check all known network exceptions
     *
     * @return predicate for checking all network exceptions
     */
    public static Predicate<Throwable> retryNetworkErrors() {
        return TarantoolRequestRetryPolicies::isNetworkError;
    }

    /**
     * Check {@link TarantoolNoSuchProcedureException}
     *
     * @return predicate for checking {@link TarantoolNoSuchProcedureException}
     */
    public static Predicate<Throwable> retryTarantoolNoSuchProcedureErrors() {
        return throwable -> throwable instanceof TarantoolNoSuchProcedureException;
    }

    private static boolean isNetworkError(Throwable e) {
        return e instanceof TimeoutException ||
            e instanceof TarantoolConnectionException ||
            e instanceof TarantoolInternalNetworkException;
    }

    private TarantoolRequestRetryPolicies() {
    }

    /**
     * Retry policy that performs unbounded number of attempts.
     * If the exception check passes, the policy returns {@code true}.
     *
     * @param <T> exception checking callback function type
     */
    public static final class InfiniteRetryPolicy<T extends Predicate<Throwable>> implements RequestRetryPolicy {

        private final long requestTimeout; //ms
        private final long operationTimeout; //ms
        private final long delay; //ms
        private final T exceptionCheck;

        /**
         * Basic constructor
         *
         * @param requestTimeout   timeout for one retry attempt, in milliseconds
         * @param operationTimeout timeout for the whole operation, in milliseconds
         * @param delay            delay between attempts, in milliseconds
         * @param exceptionCheck   predicate checking whether the given exception may be retried
         */
        public InfiniteRetryPolicy(long requestTimeout, long operationTimeout, long delay, T exceptionCheck) {
            Assert.state(requestTimeout >= 0, "Timeout must be greater or equal than 0!");
            Assert.state(operationTimeout >= requestTimeout,
                "Operation timeout must be greater or equal than requestTimeout!");
            Assert.state(delay >= 0, "Delay must be greater or equal than 0!");
            Assert.notNull(exceptionCheck, "Exception checking callback must not be null!");

            this.requestTimeout = requestTimeout;
            this.operationTimeout = operationTimeout;
            this.delay = delay;
            this.exceptionCheck = exceptionCheck;
        }

        @Override
        public boolean canRetryRequest(Throwable throwable) {
            if (testException(exceptionCheck, throwable)) {
                return true;
            }
            return false;
        }

        @Override
        public long getRequestTimeout() {
            return requestTimeout;
        }

        @Override
        public long getDelay() {
            return delay;
        }

        public long getOperationTimeout() {
            return operationTimeout;
        }

        @Override
        public <R> CompletableFuture<R> wrapOperation(Supplier<CompletableFuture<R>> operation, Executor executor) {
            Assert.notNull(operation, "Operation must not be null");
            Assert.notNull(executor, "Executor must not be null");

            // because we have asynchronous logic in completion stage chain
            // we should have sharing answer state for final result
            CompletableFuture<R> resultFuture = new CompletableFuture<>();
            // to provide it if retrying has been stopped without correct result
            AtomicReference<Throwable> lastExceptionWrapper = new AtomicReference<>();

            CompletableFuture.runAsync(() -> {
                    runAsyncOperation(operation, resultFuture, lastExceptionWrapper);
                    // set global timeout
                    ScheduledFuture<?> operationTimeoutScheduledFuture =
                        TarantoolRequestRetryPolicies.getTimeoutScheduler().schedule(() -> {
                            if (!resultFuture.isDone()) {
                                Throwable lastException = lastExceptionWrapper.get();
                                if (lastException != null) {
                                    resultFuture
                                        .completeExceptionally(
                                            new TarantoolTimeoutException(operationTimeout, lastException));
                                } else {
                                    resultFuture
                                        .completeExceptionally(new TarantoolTimeoutException(operationTimeout));
                                }
                            }
                        }, operationTimeout, TimeUnit.MILLISECONDS);
                    // optimization: stop scheduled future if resultFuture has already done
                    resultFuture.whenComplete((res, ex) -> operationTimeoutScheduledFuture.cancel(false));
                }, executor)
                .exceptionally(ex -> { // we should complete final exception if something went wrong
                    resultFuture.completeExceptionally(ex);
                    return null;
                });
            return resultFuture;
        }
    }

    /**
     * Factory for {@link InfiniteRetryPolicy}
     *
     * @param <T> exception checking predicate type
     */
    public static final class InfiniteRetryPolicyFactory<T extends Predicate<Throwable>>
        implements RequestRetryPolicyFactory {

        private final T callback;
        private final long delay; //ms
        private final long requestTimeout; //ms
        private final long operationTimeout; //ms

        /**
         * Basic constructor with timeout and delay.
         *
         * @param requestTimeout   timeout for one retry attempt, in milliseconds
         * @param operationTimeout timeout for the whole operation, in milliseconds
         * @param delay            delay between retry attempts, in milliseconds
         * @param callback         predicate checking whether the given exception may be retried
         */
        public InfiniteRetryPolicyFactory(long requestTimeout, long operationTimeout, long delay, T callback) {
            this.callback = callback;
            this.delay = delay;
            this.requestTimeout = requestTimeout;
            this.operationTimeout = operationTimeout;
        }

        /**
         * Create a builder for this factory
         *
         * @param callback predicate checking whether the given exception may be retried
         * @param <T>      exception checking callback function type
         * @return new builder instance
         */
        public static <T extends Predicate<Throwable>> Builder<T> builder(T callback) {
            return new Builder<>(callback);
        }

        @Override
        public RequestRetryPolicy create() {
            return new InfiniteRetryPolicy<>(requestTimeout, operationTimeout, delay, callback);
        }

        /**
         * Builder for {@link InfiniteRetryPolicyFactory}
         *
         * @param <T> exception checking callback function type
         */
        public static class Builder<T extends Predicate<Throwable>> {

            private long requestTimeout = DEFAULT_ONE_HOUR_TIMEOUT; //ms
            private long delay; //ms
            private final T callback;
            private long operationTimeout = DEFAULT_ONE_HOUR_TIMEOUT; //ms

            /**
             * Basic constructor
             *
             * @param callback predicate checking whether the given exception may be retried
             */
            public Builder(T callback) {
                this.callback = callback;
            }

            /**
             * Set timeout for each attempt
             *
             * @param timeout task timeout, in milliseconds
             * @return this builder instance
             */
            public Builder<T> withRequestTimeout(long timeout) {
                this.requestTimeout = timeout;
                return this;
            }

            public Builder<T> withOperationTimeout(long operationTimeout) {
                this.operationTimeout = operationTimeout;
                return this;
            }

            /**
             * Set delay between attempts
             *
             * @param delay task delay, in milliseconds
             * @return this builder instance
             */
            public Builder<T> withDelay(long delay) {
                this.delay = delay;
                return this;
            }

            /**
             * Create new factory instance
             *
             * @return new factory instance
             */
            public InfiniteRetryPolicyFactory<T> build() {
                return new InfiniteRetryPolicyFactory<>(requestTimeout, operationTimeout, delay, callback);
            }
        }

        /**
         * Getter for exception handler
         *
         * @return exception handler
         */
        public T getCallback() {
            return this.callback;
        }

        /**
         * Getter for delay
         *
         * @return delay in milliseconds
         */
        public long getDelay() {
            return this.delay;
        }

        /**
         * Getter for request timout
         *
         * @return request timeout in milliseconds
         */
        public long getRequestTimeout() {
            return this.requestTimeout;
        }

        /**
         * Getter for operation timeout
         *
         * @return operation timeout in milliseconds
         */
        public long getOperationTimeout() {
            return operationTimeout;
        }
    }

    /**
     * Retry policy that accepts a maximum number of attempts and an exception checking predicate.
     * If the exception check passes and there are any attempts left, the policy returns {@code true}.
     *
     * @param <T> exception checking predicate type
     */
    public static final class AttemptsBoundRetryPolicy<T extends Predicate<Throwable>> implements RequestRetryPolicy {

        private int attempts;
        private final int limit;
        private final long requestTimeout; //ms
        private final long delay; //ms
        private final T exceptionCheck;

        @Override
        public long getRequestTimeout() {
            return requestTimeout;
        }

        @Override
        public long getDelay() {
            return delay;
        }

        /**
         * Basic constructor with timeout
         *
         * @param attempts       maximum number of retry attempts
         * @param requestTimeout timeout for one retry attempt, in milliseconds
         * @param delay          delay between attempts, in milliseconds
         * @param exceptionCheck predicate checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicy(int attempts, long requestTimeout, long delay, T exceptionCheck) {
            Assert.state(attempts >= 0, "Attempts must be greater or equal than 0!");
            Assert.state(requestTimeout >= 0, "Timeout must be greater or equal than 0!");
            Assert.state(delay >= 0, "Timeout must be greater or equal than 0!");
            Assert.notNull(exceptionCheck, "Exception checking callback must not be null!");

            this.attempts = attempts;
            this.limit = attempts;
            this.requestTimeout = requestTimeout;
            this.delay = delay;
            this.exceptionCheck = exceptionCheck;
        }

        @Override
        public boolean canRetryRequest(Throwable throwable) {
            if (testException(exceptionCheck, throwable) && attempts > 0) {
                attempts--;
                return true;
            }
            return false;
        }

        @Override
        public <R> void runAsyncOperation(
            Supplier<CompletableFuture<R>> operation, CompletableFuture<R> resultFuture,
            AtomicReference<Throwable> lastExceptionWrapper) {
            // start async operation running
            CompletableFuture<R> operationFuture = operation.get();
            // start scheduled request timeout task
            // it never completes correctly only exceptionally
            CompletableFuture<R> requestTimeoutFuture = failAfterRequestTimeout(resultFuture);

            operationFuture.acceptEither(requestTimeoutFuture, resultFuture::complete)
                .exceptionally(ex -> { // if requestTimeout has been raised or operation return exception

                    while (ex instanceof ExecutionException || ex instanceof CompletionException) {
                        ex = ex.getCause();
                    }
                    // to provide it if retrying has been stopped without correct result
                    lastExceptionWrapper.set(ex);

                    if (attempts == 0) {
                        ex = new TarantoolAttemptsLimitException(
                            limit,
                            ex);
                        resultFuture.completeExceptionally(ex);
                        return null;
                    }

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
    }

    /**
     * Factory for {@link AttemptsBoundRetryPolicy}
     *
     * @param <T> exception checking predicate type
     */
    public static final class AttemptsBoundRetryPolicyFactory<T extends Predicate<Throwable>>
        implements RequestRetryPolicyFactory {

        private final int numberOfAttempts;
        private final T exceptionCheck;
        private final long delay; //ms
        private final long requestTimeout; //ms

        /**
         * Basic constructor with timeout and delay
         *
         * @param numberOfAttempts maximum number of retry attempts
         * @param requestTimeout   timeout for one retry attempt, in milliseconds
         * @param delay            delay between retry attempts, in milliseconds
         * @param exceptionCheck   predicate checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicyFactory(
            int numberOfAttempts,
            long requestTimeout,
            long delay,
            T exceptionCheck) {
            this.numberOfAttempts = numberOfAttempts;
            this.requestTimeout = requestTimeout;
            this.delay = delay;
            this.exceptionCheck = exceptionCheck;
        }

        /**
         * Create a builder for this factory
         *
         * @param attempts       maximum number of attempts
         * @param exceptionCheck function checking whether the given exception may be retried
         * @param <T>            exception checking predicate type
         * @return new builder instance
         */
        public static <T extends Predicate<Throwable>> Builder<T> builder(int attempts, T exceptionCheck) {
            return new Builder<>(attempts, exceptionCheck);
        }

        @Override
        public RequestRetryPolicy create() {
            return new AttemptsBoundRetryPolicy<>(numberOfAttempts, requestTimeout, delay, exceptionCheck);
        }

        /**
         * Builder for {@link AttemptsBoundRetryPolicyFactory}
         *
         * @param <T> exception checking predicate type
         */
        public static class Builder<T extends Predicate<Throwable>> {
            private final int numberOfAttempts;
            private long requestTimeout = DEFAULT_ONE_HOUR_TIMEOUT; //ms
            private long delay; //ms
            private final T exceptionCheck;

            /**
             * Basic constructor
             *
             * @param numberOfAttempts maximum number of retry attempts
             * @param exceptionCheck   predicate checking whether the given exception may be retried
             */
            public Builder(int numberOfAttempts, T exceptionCheck) {
                this.numberOfAttempts = numberOfAttempts;
                this.exceptionCheck = exceptionCheck;
            }

            /**
             * Set timeout for each attempt
             *
             * @param requestTimeout task timeout, in milliseconds
             * @return this builder instance
             */
            public Builder<T> withRequestTimeout(long requestTimeout) {
                this.requestTimeout = requestTimeout;
                return this;
            }

            /**
             * Set delay between attempts
             *
             * @param delay task delay, in milliseconds
             * @return this builder instance
             */
            public Builder<T> withDelay(long delay) {
                this.delay = delay;
                return this;
            }

            /**
             * Create new factory instance
             *
             * @return new factory instance
             */
            public AttemptsBoundRetryPolicyFactory<T> build() {
                return new AttemptsBoundRetryPolicyFactory<>(numberOfAttempts, requestTimeout, delay, exceptionCheck);
            }
        }

        /**
         * Getter for number of attempts
         *
         * @return number of attempts
         */
        public int getNumberOfAttempts() {
            return numberOfAttempts;
        }

        /**
         * Getter for exception handler
         *
         * @return exception handler
         */
        public T getExceptionCheck() {
            return exceptionCheck;
        }

        /**
         * Getter for delay
         *
         * @return delay in milliseconds
         */
        public long getDelay() {
            return delay;
        }

        /**
         * Getter for request timeout
         *
         * @return request timeout in milliseconds
         */
        public long getRequestTimeout() {
            return requestTimeout;
        }
    }

    /**
     * Create a factory for retry policy bound by retry attempts.
     * The retry will be performed on any known network exceptions.
     *
     * @param numberOfAttempts maximum number of retries, zero value means no retries
     * @return new factory instance
     */
    public static AttemptsBoundRetryPolicyFactory.Builder<Predicate<Throwable>>
    byNumberOfAttempts(int numberOfAttempts) {
        return byNumberOfAttempts(numberOfAttempts, retryNetworkErrors());
    }

    /**
     * Create a factory for retry policy bound by retry attempts
     *
     * @param numberOfAttempts maximum number of retries, zero value means no retries
     * @param exceptionCheck   predicate, checking the given exception whether the request may be retried
     * @param <T>              exception checking predicate type
     * @return new factory instance
     */
    public static <T extends Predicate<Throwable>> AttemptsBoundRetryPolicyFactory.Builder<T>
    byNumberOfAttempts(int numberOfAttempts, T exceptionCheck) {
        return AttemptsBoundRetryPolicyFactory.builder(numberOfAttempts, exceptionCheck);
    }

    /**
     * Create a factory for retry policy with unbounded number of attempts.
     * {@link #retryNetworkErrors} is used for checking exceptions by default.
     *
     * @param <T> exception checking predicate type
     * @return new factory instance
     */
    @SuppressWarnings("unchecked")
    public static <T extends Predicate<Throwable>> InfiniteRetryPolicyFactory.Builder<T> unbound() {
        return unbound((T) retryNetworkErrors());
    }

    /**
     * Create a factory for retry policy with unbounded number of attempts
     *
     * @param exceptionCheck predicate, checking the given exception whether the request may be retried
     * @param <T>            exception checking predicate type
     * @return new factory instance
     */
    public static <T extends Predicate<Throwable>> InfiniteRetryPolicyFactory.Builder<T>
    unbound(T exceptionCheck) {
        return InfiniteRetryPolicyFactory.builder(exceptionCheck);
    }

    private static boolean testException(Predicate<Throwable> exceptionCheck, Throwable throwable) {
        try {
            return exceptionCheck.test(throwable);
        } catch (Exception e) {
            throw new TarantoolClientException(
                "Specified in TarantoolClient predicate for exception check threw exception: ", e);
        }
    }

    /**
     * Get timeout scheduler instance.
     *
     * @return scheduler instance
     */
    public static ScheduledExecutorService getTimeoutScheduler() {
        return timeoutScheduler;
    }
}
