package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;

/**
 * Marker interface for space update operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface UpdateOptions<T extends UpdateOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
