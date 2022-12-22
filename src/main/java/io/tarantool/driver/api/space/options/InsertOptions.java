package io.tarantool.driver.api.space.options;

/**
 * Marker interface for space insert operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface InsertOptions<T extends InsertOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
