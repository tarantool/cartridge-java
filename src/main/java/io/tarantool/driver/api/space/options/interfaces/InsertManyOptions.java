package io.tarantool.driver.api.space.options.interfaces;

import io.tarantool.driver.api.space.options.contracts.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithRollbackOnErrorOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithStopOnErrorOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithTimeoutOptions;

/**
 * Marker interface for space insert_many operation options
 *
 * @author Alexey Kuzin
 */
public interface InsertManyOptions<T extends InsertManyOptions<T>>
    extends OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>, OperationWithRollbackOnErrorOptions<T>,
        OperationWithStopOnErrorOptions<T> {
}
