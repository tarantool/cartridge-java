package io.tarantool.driver.api.space.options;

/**
 * Marker interface for space update operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface UpdateOptions<T extends UpdateOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T> {
}
