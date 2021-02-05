package io.tarantool.driver;

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
        private final T callback;

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

        @Override
        public RequestRetryPolicy create() {
            return new AttemptsBoundRetryPolicy<>(numberOfAttempts, callback);
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
}
