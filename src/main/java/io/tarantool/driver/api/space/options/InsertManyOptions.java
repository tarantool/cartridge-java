package io.tarantool.driver.api.space.options;

/**
 * Marker interface for space insert_many operation options
 *
 * @author Alexey Kuzin
 */
public interface InsertManyOptions<T extends InsertManyOptions<T>>
    extends OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>, OperationWithRollbackOnErrorOptions<T>,
    OperationWithStopOnErrorOptions<T> {
}
