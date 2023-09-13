package io.tarantool.driver.api.space.options.interfaces;

import io.tarantool.driver.api.space.options.contracts.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithTimeoutOptions;

/**
 * Marker interface for space replace operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface ReplaceOptions<T extends ReplaceOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
