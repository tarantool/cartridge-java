package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Class-container for built-in request retry policies
 *
 * @author Alexey Kuzin
 */
public final class TarantoolRequestRetryPolicies {

    private static final Function<Throwable, Boolean> retryAll = t -> true;

    private TarantoolRequestRetryPolicies() {
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
        private final long timeout; //ms
        private final long delay; //ms
        private final T callback;

        @Override
        public long getOperationTimeout() {
            return timeout;
        }

        /**
         * Basic constructor with timeout
         *
         * @param attempts  maximum number of retry attempts
         * @param timeout   timeout for one retry attempt, in milliseconds
         * @param delay     delay between attempts, in milliseconds
         * @param callback  function checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicy(int attempts, long timeout, long delay, T callback) {
            Assert.state(attempts >= 0, "Attempts must be greater or equal than 0!");
            Assert.state(timeout >= 0, "Timeout must be greater or equal than 0!");
            Assert.state(delay >= 0, "Timeout must be greater or equal than 0!");
            Assert.notNull(callback, "Callback must not be null!");

            this.attempts = attempts;
            this.timeout = timeout;
            this.delay = delay;
            this.callback = callback;
        }

        @Override
        public boolean canRetryRequest(Throwable throwable) {
            if (callback.apply(throwable) && attempts > 0) {
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
    }

    /**
     * Factory for {@link AttemptsBoundRetryPolicy}
     *
     * @param <T> exception checking callback function type
     */
    public static final class AttemptsBoundRetryPolicyFactory<T extends Function<Throwable, Boolean>>
            implements RequestRetryPolicyFactory {

        private final int numberOfAttempts;
        private final T callback;
        private final long delay; //ms
        private final long timeout; //ms

        /**
         * Basic constructor with timeout and delay
         *
         * @param numberOfAttempts  maximum number of retry attempts
         * @param timeout           timeout for one retry attempt, in milliseconds
         * @param delay             delay between retry attempts, in milliseconds
         * @param callback          function checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicyFactory(int numberOfAttempts, long timeout, long delay, T callback) {
            this.numberOfAttempts = numberOfAttempts;
            this.timeout = timeout;
            this.delay = delay;
            this.callback = callback;
        }

        /**
         * Create a builder for this factory
         *
         * @param attempts  maximum number of attempts
         * @param callback  function checking whether the given exception may be retried
         * @param <T>       exception checking callback function type
         * @return new builder instance
         */
        public static <T extends Function<Throwable, Boolean>> Builder<T> builder(int attempts, T callback) {
            return new Builder<>(attempts, callback);
        }

        @Override
        public RequestRetryPolicy create() {
            return new AttemptsBoundRetryPolicy<>(numberOfAttempts, timeout, delay, callback);
        }

        /**
         * Builder for {@link AttemptsBoundRetryPolicyFactory}
         *
         * @param <T> exception checking callback function type
         */
        public static class Builder<T extends Function<Throwable, Boolean>> {
            private final int numberOfAttempts;
            private long timeout = TimeUnit.HOURS.toMillis(1); //ms
            private long delay = 0; //ms
            private final T callback;

            /**
             * Basic constructor
             *
             * @param numberOfAttempts  maximum number of retry attempts
             * @param callback          function checking whether the given exception may be retried
             */
            public Builder(int numberOfAttempts, T callback) {
                this.numberOfAttempts = numberOfAttempts;
                this.callback = callback;
            }

            /**
             * Set timeout for each attempt
             *
             * @param timeout   task timeout, in milliseconds
             * @return this builder instance
             */
            public Builder<T> withTimeout(long timeout) {
                this.timeout = timeout;
                return this;
            }

            /**
             * Set delay between attempts
             *
             * @param delay   task delay, in milliseconds
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
                return new AttemptsBoundRetryPolicyFactory<>(numberOfAttempts, timeout, delay, callback);
            }
        }
    }

    /**
     * Create a factory for retry policy bound by retry attempts. The retry will be performed on any exception.
     *
     * @param numberOfAttempts  maximum number of retries, zero value means no retries
     * @return new factory instance
     */
    public static
    AttemptsBoundRetryPolicyFactory.Builder<Function<Throwable, Boolean>>
    byNumberOfAttempts(int numberOfAttempts) {
        return AttemptsBoundRetryPolicyFactory.builder(numberOfAttempts, retryAll);
    }

    /**
     * Create a factory for retry policy bound by retry attempts
     *
     * @param numberOfAttempts  maximum number of retries, zero value means no retries
     * @param exceptionCheck    function callback, checking the given exception whether the request may be retried
     * @param <T> exception checking callback function type
     * @return new factory instance
     */
    public static
    <T extends Function<Throwable, Boolean>> AttemptsBoundRetryPolicyFactory.Builder<T>
    byNumberOfAttempts(int numberOfAttempts, T exceptionCheck) {
        return AttemptsBoundRetryPolicyFactory.builder(numberOfAttempts, exceptionCheck);
    }
}
