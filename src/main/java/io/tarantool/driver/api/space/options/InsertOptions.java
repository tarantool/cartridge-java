package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;

/**
 * Marker interface for space insert operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface InsertOptions<T extends InsertOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
