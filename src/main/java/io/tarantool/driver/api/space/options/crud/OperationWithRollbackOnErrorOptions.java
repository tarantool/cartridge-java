package io.tarantool.driver.api.space.options.crud;


import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;
import io.tarantool.driver.api.space.options.crud.enums.RollbackOnError;
import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable rollback_on_error.
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithRollbackOnErrorOptions<T extends OperationWithRollbackOnErrorOptions<T>>
    extends Options, Self<T> {

    /**
     * Specifies whether to not save any changes in the space if any tuple replace operation
     * is unsuccesful. Default value is <code>true</code>.
     *
     * @param rollbackOnError should rollback batch on error
     * @return this options instance
     */
    default T withRollbackOnError(RollbackOnError rollbackOnError) {
        addOption(ProxyOption.ROLLBACK_ON_ERROR, rollbackOnError.value());
        return self();
    }

    /**
     * Return whether all changes should not be saved if any tuple replace
     * was unsuccesful.
     *
     * @return true, if the operation should rollback on error
     */
    default Optional<Boolean> getRollbackOnError() {
        return getOption(ProxyOption.ROLLBACK_ON_ERROR, Boolean.class);
    }
}
