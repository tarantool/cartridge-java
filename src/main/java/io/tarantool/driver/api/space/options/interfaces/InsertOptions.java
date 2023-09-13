package io.tarantool.driver.api.space.options.interfaces;

import io.tarantool.driver.api.space.options.contracts.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithTimeoutOptions;

/**
 * Marker interface for space insert operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface InsertOptions<T extends InsertOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
