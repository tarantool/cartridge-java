package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithRollbackOnErrorOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithStopOnErrorOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;

/**
 * Marker interface for space replace_many operation options
 *
 * @author Alexey Kuzin
 */
public interface ReplaceManyOptions<T extends ReplaceManyOptions<T>>
    extends OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>, OperationWithRollbackOnErrorOptions<T>,
        OperationWithStopOnErrorOptions<T> {
}
