package io.tarantool.driver.retry;

import io.tarantool.driver.exceptions.TarantoolAttemptsLimitException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolServerInternalNetworkException;
import io.tarantool.driver.exceptions.TarantoolTimeoutException;
import io.tarantool.driver.exceptions.TarantoolConnectionException;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Class-container for built-in request retry policies
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class TarantoolRequestRetryPolicies {

    public static Function<Throwable, Boolean> retryAll = t -> true;
    public static Function<Throwable, Boolean> retryNone = t -> false;
    public static <T extends Function<Throwable, Boolean>> Function<Throwable, Boolean>
    withRetryingNetworkErrors(T exceptionCheck) {
        return e -> {
            boolean retryRequest = false;
            Boolean userExceptionCheck = exceptionCheck.apply(e);
            if (e instanceof TimeoutException ||
                    e instanceof TarantoolConnectionException ||
                    e instanceof TarantoolServerInternalNetworkException) {
                retryRequest = true;
            }
            return retryRequest || userExceptionCheck;
        };
    }
    public static Function<Throwable, Boolean> retryNetworkErrors() {
        return withRetryingNetworkErrors(retryNone);
    }

    private TarantoolRequestRetryPolicies() {
    }

    /**
     * Retry policy that performs unbounded number of attempts.
     * If the exception check passes, the policy returns {@code true}.
     *
     * @param <T> exception checking callback function type
     */
    public static final class InfiniteRetryPolicy<T extends Function<Throwable, Boolean>>
            implements RequestRetryPolicy {

        private final long requestTimeout; //ms
        private final long operationTimeout; //ms
        private final long delay; //ms
        private final T callback;

        /**
         * Basic constructor
         *
         * @param requestTimeout   timeout for one retry attempt, in milliseconds
         * @param operationTimeout timeout for the whole operation, in milliseconds
         * @param delay            delay between attempts, in milliseconds
         * @param exceptionCheck   function checking whether the given exception may be retried
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
            this.callback = exceptionCheck;
        }

        @Override
        public boolean canRetryRequest(Throwable throwable) {
            if (callback.apply(throwable)) {
                if (delay > 0) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                    } catch (InterruptedException e) {
                        throw new TarantoolClientException("Request retry delay has been interrupted");
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public long getRequestTimeout() {
            return requestTimeout;
        }

        public long getOperationTimeout() {
            return operationTimeout;
        }

        @Override
        public <T> CompletableFuture<T> wrapOperation(Supplier<CompletableFuture<T>> operation, Executor executor) {

            Assert.notNull(operation, "Operation must not be null");
            Assert.notNull(executor, "Executor must not be null");

            return CompletableFuture.supplyAsync(() -> {
                long timeElapsed = 0;
                long tStart;
                Throwable ex;
                do {
                    tStart = System.nanoTime();
                    try {
                        return operation.get().get(getRequestTimeout(), TimeUnit.MILLISECONDS);
                    } catch (TimeoutException | InterruptedException e) {
                        ex = e;
                    } catch (ExecutionException e) {
                        ex = e.getCause();
                    }
                    timeElapsed = timeElapsed + (System.nanoTime() - tStart) / 1_000_000L;
                    if (timeElapsed >= getOperationTimeout()) {
                        ex = new TarantoolTimeoutException(
                                timeElapsed,
                                ex);
                        break;
                    }
                } while (this.canRetryRequest(ex));
                throw new CompletionException(ex);
            }, executor);
        }
    }

    /**
     * Factory for {@link InfiniteRetryPolicy}
     *
     * @param <T> exception checking callback function type
     */
    public static final class InfiniteRetryPolicyFactory<T extends Function<Throwable, Boolean>>
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
         * @param callback         function checking whether the given exception may be retried
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
         * @param callback function checking whether the given exception may be retried
         * @param <T>      exception checking callback function type
         * @return new builder instance
         */
        public static <T extends Function<Throwable, Boolean>> Builder<T> builder(T callback) {
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
        public static class Builder<T extends Function<Throwable, Boolean>> {

            private long requestTimeout = TimeUnit.HOURS.toMillis(1); //ms
            private long delay; //ms
            private final T callback;
            private long operationTimeout = TimeUnit.HOURS.toMillis(1); //ms

            /**
             * Basic constructor
             *
             * @param callback function checking whether the given exception may be retried
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
    }

    /**
     * Retry policy that accepts a maximum number of attempts and an exception checking callback.
     * If the exception check passes and there are any attempts left, the policy returns {@code true}.
     *
     * @param <T> exception checking callback function type
     */
    public static final class AttemptsBoundRetryPolicy<T extends Function<Throwable, Boolean>>
            implements RequestRetryPolicy {

        private int attempts;
        private final int limit;
        private final long requestTimeout; //ms
        private final long delay; //ms
        private final T exceptionCheck;

        @Override
        public long getRequestTimeout() {
            return requestTimeout;
        }

        /**
         * Basic constructor with timeout
         *
         * @param attempts       maximum number of retry attempts
         * @param requestTimeout timeout for one retry attempt, in milliseconds
         * @param delay          delay between attempts, in milliseconds
         * @param exceptionCheck function checking whether the given exception may be retried
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
            if (exceptionCheck.apply(throwable) && attempts > 0) {
                attempts--;
                if (delay > 0) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                    } catch (InterruptedException e) {
                        throw new TarantoolClientException("Request retry delay has been interrupted");
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public <T> CompletableFuture<T> wrapOperation(Supplier<CompletableFuture<T>> operation, Executor executor) {

            Assert.notNull(operation, "Operation must not be null");
            Assert.notNull(executor, "Executor must not be null");

            return CompletableFuture.supplyAsync(() -> {
                Throwable ex;
                do {
                    try {
                        return operation.get().get(getRequestTimeout(), TimeUnit.MILLISECONDS);
                    } catch (TimeoutException | InterruptedException e) {
                        ex = e;
                    } catch (ExecutionException e) {
                        ex = e.getCause();
                    }
                    if (attempts == 0) {
                        ex = new TarantoolAttemptsLimitException(
                                limit,
                                ex);
                        break;
                    }
                } while (this.canRetryRequest(ex));
                throw new CompletionException(ex);
            }, executor);
        }
    }

    /**
     * Factory for {@link AttemptsBoundRetryPolicy}
     *
     * @param <T> exception checking callback function type
     */
    public static final class AttemptsBoundRetryPolicyFactory<T extends Function<Throwable, Boolean>>
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
         * @param exceptionCheck   function checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicyFactory(int numberOfAttempts,
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
         * @param <T>            exception checking callback function type
         * @return new builder instance
         */
        public static <T extends Function<Throwable, Boolean>> Builder<T> builder(int attempts, T exceptionCheck) {
            return new Builder<>(attempts, exceptionCheck);
        }

        @Override
        public RequestRetryPolicy create() {
            return new AttemptsBoundRetryPolicy<>(numberOfAttempts, requestTimeout, delay, exceptionCheck);
        }

        /**
         * Builder for {@link AttemptsBoundRetryPolicyFactory}
         *
         * @param <T> exception checking callback function type
         */
        public static class Builder<T extends Function<Throwable, Boolean>> {
            private final int numberOfAttempts;
            private long requestTimeout = TimeUnit.HOURS.toMillis(1); //ms
            private long delay; //ms
            private final T exceptionCheck;

            /**
             * Basic constructor
             *
             * @param numberOfAttempts maximum number of retry attempts
             * @param exceptionCheck   function checking whether the given exception may be retried
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
    }

    /**
     * Create a factory for retry policy bound by retry attempts. The retry will be performed on any exception.
     *
     * @param numberOfAttempts maximum number of retries, zero value means no retries
     * @return new factory instance
     */
    public static AttemptsBoundRetryPolicyFactory.Builder<Function<Throwable, Boolean>>
    byNumberOfAttempts(int numberOfAttempts) {
        return byNumberOfAttempts(numberOfAttempts, retryAll);
    }

    /**
     * Create a factory for retry policy bound by retry attempts
     *
     * @param numberOfAttempts maximum number of retries, zero value means no retries
     * @param exceptionCheck   function callback, checking the given exception whether the request may be retried
     * @param <T>              exception checking callback function type
     * @return new factory instance
     */
    public static <T extends Function<Throwable, Boolean>> AttemptsBoundRetryPolicyFactory.Builder<T>
    byNumberOfAttempts(int numberOfAttempts, T exceptionCheck) {
        return AttemptsBoundRetryPolicyFactory.builder(numberOfAttempts, exceptionCheck);
    }

    /**
     * Create a factory for retry policy with unbounded number of attempts
     *
     * @param <T> exception checking callback function type
     * @return new factory instance
     */
    @SuppressWarnings("unchecked")
    public static <T extends Function<Throwable, Boolean>> InfiniteRetryPolicyFactory.Builder<T>
    unbound() {
        return unbound((T) retryAll);
    }

    /**
     * Create a factory for retry policy with unbounded number of attempts
     *
     * @param exceptionCheck function callback, checking the given exception whether the request may be retried
     * @param <T>            exception checking callback function type
     * @return new factory instance
     */
    public static <T extends Function<Throwable, Boolean>> InfiniteRetryPolicyFactory.Builder<T>
    unbound(T exceptionCheck) {
        return InfiniteRetryPolicyFactory.builder(exceptionCheck);
    }
}
