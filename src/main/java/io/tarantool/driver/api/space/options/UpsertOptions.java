package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;

/**
 * Marker interface for space upsert operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface UpsertOptions<T extends UpsertOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
