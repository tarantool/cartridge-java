package io.tarantool.driver.api.space.options;

/**
 * Marker interface for space replace operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface ReplaceOptions<T extends ReplaceOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T> {
}
