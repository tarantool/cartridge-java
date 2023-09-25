package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;

/**
 * Marker interface for space replace operation options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface ReplaceOptions<T extends ReplaceOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T> {
}
