package io.tarantool.driver.api.space.options.proxy;

import java.util.Optional;

import io.tarantool.driver.api.space.options.AbstractOptions;
import io.tarantool.driver.api.space.options.OperationWithTimeoutOptions;

/**
 * Represent options for all proxy functions
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
abstract class ProxyBaseOptions<B extends ProxyBaseOptions<B>> extends AbstractOptions<B>
    implements OperationWithTimeoutOptions {

    public static final String TIMEOUT = "timeout";

    /**
     * Specifies timeout for waiting for a server response for the operation.
     * Configured request timeout for that client will be used by default.
     *
     * @param timeout request timeout, in milliseconds
     * @return this options instance
     */
    public B withTimeout(int timeout) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout should be greater than 0");
        }
        addOption(TIMEOUT, timeout);
        return self();
    }

    @Override
    public Optional<Integer> getTimeout() {
        return getOption(TIMEOUT, Integer.class);
    }
}
