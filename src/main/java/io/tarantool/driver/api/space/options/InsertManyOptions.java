package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithRollbackOnErrorOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithStopOnErrorOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithVshardRouterOptions;

/**
 * Marker interface for space insert_many operation options
 *
 * @author Alexey Kuzin
 */
public interface InsertManyOptions<T extends InsertManyOptions<T>>
    extends OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>, OperationWithRollbackOnErrorOptions<T>,
            OperationWithStopOnErrorOptions<T>, OperationWithVshardRouterOptions<T> {
}
