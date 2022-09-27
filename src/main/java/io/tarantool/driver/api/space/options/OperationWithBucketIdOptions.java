package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable bucket id.
 *
 * @author Artyom Dubinin
 */
public interface OperationWithBucketIdOptions extends OperationWithTimeoutOptions {
    /**
     * Return bucket id that is used by operation to find storage location.
     *
     * @return bucket id
     */
    Optional<Integer> getBucketId();
}
