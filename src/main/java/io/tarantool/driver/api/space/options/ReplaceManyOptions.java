package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Marker interface for space replace_many operation options
 *
 * @author Alexey Kuzin
 */
public interface ReplaceManyOptions<T extends ReplaceManyOptions<T>>
    extends OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>, OperationWithRollbackOnErrorOptions<T> {

    /**
     * Return whether the operation should be interrupted if any tuple replace
     * was unsuccesful.
     *
     * @return true, if the operation should stop on error
     */
    Optional<Boolean> getStopOnError();
}
