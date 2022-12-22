package io.tarantool.driver.api.space.options;

/**
 * Marker interface for space upsert operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface UpsertOptions<T extends UpsertOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
