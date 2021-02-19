package io.tarantool.driver;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Class-container for default kinds of request retry policies
 *
 * @author Alexey Kuzin
 */
public final class TarantoolRequestRetryPolicies {

    private TarantoolRequestRetryPolicies() {
    }

    /**
     * Retry policy shich accepts a maximum number of attempts and an exception checking callback.
     * If the exception check passes and there are any attempts left, the policy returns {@code true}.
     *
     * @param <T> exception checking callback function type
     */
    public static final class AttemptsBoundRetryPolicy<T extends Function<Throwable, Boolean>>
            implements RequestRetryPolicy {

        private int attempts;
        private long timeout = TimeUnit.HOURS.toMillis(1);
        private final T callback;

        @Override
        public long getOperationTimeout() {
            return timeout;
        }

        /**
         * Basic constructor
         *
         * @param attempts maximum number of retry attempts
         * @param callback function checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicy(int attempts, T callback) {
            this.attempts = attempts;
            this.callback = callback;
        }

        /**
         * Basic constructor with timeout
         *
         * @param attempts  maximum number of retry attempts
         * @param timeout   timeout for one retry attempt, in milliseconds
         * @param callback  function checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicy(int attempts, long timeout, T callback) {
            this.attempts = attempts;
            this.callback = callback;
            this.timeout = timeout;
        }

        @Override
        public boolean canRetryRequest(Throwable throwable) {
            if (callback.apply(throwable) && attempts > 0) {
                attempts--;
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
        private long timeout = TimeUnit.HOURS.toMillis(1) ;
        private final T callback;

        /**
         * Basic constructor
         *
         * @param numberOfAttempts  maximum number of retry attempts
         * @param callback          function checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicyFactory(int numberOfAttempts, T callback) {
            this.numberOfAttempts = numberOfAttempts;
            this.callback = callback;
        }

        /**
         * Basic constructor with timeout
         *
         * @param numberOfAttempts  maximum number of retry attempts
         * @param timeout           timeout for one retry attempt, in milliseconds
         * @param callback          function checking whether the given exception may be retried
         */
        public AttemptsBoundRetryPolicyFactory(int numberOfAttempts, long timeout, T callback) {
            this.numberOfAttempts = numberOfAttempts;
            this.timeout = timeout;
            this.callback = callback;
        }

        @Override
        public RequestRetryPolicy create() {
            return new AttemptsBoundRetryPolicy<>(numberOfAttempts, timeout, callback);
        }
    }

    /**
     * Create a factory for retry policy bound by retry attempts
     *
     * @param numberOfAttempts  maximum number of retries
     * @param exceptionCheck    function callback, checking the given exception whether the request may be retried
     * @param <T> exception checking callback function type
     * @return new factory instance
     */
    public static
    <T extends Function<Throwable, Boolean>> AttemptsBoundRetryPolicyFactory<T>
    byNumberOfAttempts(int numberOfAttempts, T exceptionCheck) {
        return new AttemptsBoundRetryPolicyFactory<>(numberOfAttempts, exceptionCheck);
    }

    /**
     * Create a factory for retry policy bound by retry attempts. Limit time for each attempt with the given timeout
     * value
     *
     * @param numberOfAttempts  maximum number of retries
     * @param timeout           timeout for one retry attempt, in milliseconds
     * @param exceptionCheck    function callback, checking the given exception whether the request may be retried
     * @param <T> exception checking callback function type
     * @return new factory instance
     */
    public static
    <T extends Function<Throwable, Boolean>> AttemptsBoundRetryPolicyFactory<T>
    byNumberOfAttempts(int numberOfAttempts, long timeout, T exceptionCheck) {
        return new AttemptsBoundRetryPolicyFactory<>(numberOfAttempts, timeout, exceptionCheck);
    }
}
