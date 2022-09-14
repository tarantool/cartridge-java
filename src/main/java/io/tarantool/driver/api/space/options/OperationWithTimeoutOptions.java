package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Base class for all operation options that may have a configurable timeout.
 *
 * @author Alexey Kuzin
 */
public interface OperationWithTimeoutOptions extends Options {
    /**
     * Return operation timeout.
     *
     * @return timeout, in milliseconds.
     */
    Optional<Integer> getTimeout();
}
