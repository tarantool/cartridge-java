package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Marker interface for space insert_many operation options
 *
 * @author Alexey Kuzin
 */
public interface InsertManyOptions extends OperationWithTimeoutOptions {
    /**
     * Return whether all changes should not be saved if any tuple insertion
     * was unsuccesful.
     *
     * @return true, if the operation should rollback on error
     */
    Optional<Boolean> getRollbackOnError();

    /**
     * Return whether the operation should be interrupted if any tuple insertion
     * was unsuccesful.
     *
     * @return true, if the operation should stop on error
     */
    Optional<Boolean> getStopOnError();
}
