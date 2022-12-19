package io.tarantool.driver.api.space.options;

/**
 * Marker interface for space delete operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface DeleteOptions<T extends DeleteOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T> {
}
