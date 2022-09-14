package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Marker interface for space select operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface SelectOptions extends OperationWithTimeoutOptions {
    /**
     * Return the internal size of batch for transferring data between
     * storage and router nodes.
     *
     * @return batch size
     */
    Optional<Integer> getBatchSize();
}
