package io.tarantool.driver.api.space.options;

/**
 * Marker interface for space replace_many operation options
 *
 * @author Alexey Kuzin
 */
public interface ReplaceManyOptions<T extends ReplaceManyOptions<T>>
    extends OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>, OperationWithRollbackOnErrorOptions<T>,
    OperationWithStopOnErrorOptions<T> {
}
