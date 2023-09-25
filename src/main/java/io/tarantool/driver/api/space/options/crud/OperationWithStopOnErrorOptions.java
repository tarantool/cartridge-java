package io.tarantool.driver.api.space.options.crud;

import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;
import io.tarantool.driver.api.space.options.crud.enums.StopOnError;
import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable stop_on_error.
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithStopOnErrorOptions<T extends OperationWithStopOnErrorOptions<T>>
    extends Options, Self<T> {

    /**
     * Specifies whether to not try to insert more tuples into the space if any tuple insert
     * operation is unsuccesful. Default value is <code>true</code>.
     *
     * @param stopOnError should stop batch on error
     * @return this options instance
     */
    default T withStopOnError(StopOnError stopOnError) {
        addOption(ProxyOption.STOP_ON_ERROR, stopOnError.value());
        return self();
    }

    /**
     * Return whether the operation should be interrupted if any tuple replace
     * was unsuccesful.
     *
     * @return true, if the operation should stop on error
     */
    default Optional<Boolean> getStopOnError() {
        return getOption(ProxyOption.STOP_ON_ERROR, Boolean.class);
    }
}
