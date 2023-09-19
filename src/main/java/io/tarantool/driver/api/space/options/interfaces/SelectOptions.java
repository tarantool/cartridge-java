package io.tarantool.driver.api.space.options.interfaces;

import io.tarantool.driver.api.space.options.contracts.OperationWithBatchSizeOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithModeOptions;
import io.tarantool.driver.api.space.options.contracts.OperationWithTimeoutOptions;

/**
 * Marker interface for space select operation options
 * <p>TODO: separate proxy options and cluster options:
 * <a href="https://github.com/tarantool/cartridge-java/issues/425">issue</a></p>
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface SelectOptions<T extends SelectOptions<T>>
    extends OperationWithBucketIdOptions<T>, OperationWithTimeoutOptions<T>, OperationWithFieldsOptions<T>,
    OperationWithModeOptions<T>, OperationWithBatchSizeOptions<T> {
}
