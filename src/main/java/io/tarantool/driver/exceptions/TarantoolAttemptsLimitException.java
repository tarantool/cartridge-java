package io.tarantool.driver.exceptions;

import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

/**
 * The exception that was thrown from {@link TarantoolRequestRetryPolicies}
 *
 * @author Artyom Dubinin
 */
public class TarantoolAttemptsLimitException extends TarantoolException {
    private static final String message = "Attempts limit reached: %s";

    public TarantoolAttemptsLimitException(Throwable cause) {
        super(cause);
    }

    public TarantoolAttemptsLimitException(Integer limit) {
        super(String.format(message, limit));
    }

    public TarantoolAttemptsLimitException(Integer limit, Throwable cause) {
        super(String.format(message, limit), cause);
    }

    public TarantoolAttemptsLimitException(String format, Object... args) {
        super(String.format(format, args));
    }
}
