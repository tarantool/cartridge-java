package io.tarantool.driver.api.space.options.interfaces;

import io.tarantool.driver.api.space.options.contracts.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithRollbackOnErrorOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithStopOnErrorOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithTimeoutOptions;

/**
 * Marker interface for space replace_many operation options
 *
 * @author Alexey Kuzin
 */
public interface ReplaceManyOptions<T extends ReplaceManyOptions<T>>
    extends OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>, OperationWithRollbackOnErrorOptions<T>,
        OperationWithStopOnErrorOptions<T> {
}
