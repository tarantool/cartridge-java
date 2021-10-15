package io.tarantool.driver.exceptions;

import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;

/**
 * The exception that was thrown from {@link TarantoolRequestRetryPolicies}
 *
 * @author Artyom Dubinin
 */
public class TarantoolTimeoutException extends TarantoolException {
    private static final String message = "Operation timeout value exceeded after %s ms";

    public TarantoolTimeoutException(Throwable cause) {
        super(cause);
    }

    public TarantoolTimeoutException(Long time) {
        super(String.format(message, time));
    }

    public TarantoolTimeoutException(Long time, Throwable cause) {
        super(String.format(message, time), cause);
    }

    public TarantoolTimeoutException(String format, Object... args) {
        super(String.format(format, args));
    }
}
