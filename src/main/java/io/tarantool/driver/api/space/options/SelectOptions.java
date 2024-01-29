package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.OperationWithBalanceOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithBatchSizeOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithBucketIdOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithFetchLatestMetadataOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithFieldsOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithFullScanOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithForceMapCallOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithModeOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithPreferReplicaOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithVshardRouterOptions;
import io.tarantool.driver.api.space.options.crud.OperationWithYieldEveryOptions;

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
            OperationWithModeOptions<T>, OperationWithBatchSizeOptions<T>, OperationWithYieldEveryOptions<T>,
            OperationWithForceMapCallOptions<T>, OperationWithFullScanOptions<T>, OperationWithPreferReplicaOptions<T>,
            OperationWithBalanceOptions<T>, OperationWithVshardRouterOptions<T>,
            OperationWithFetchLatestMetadataOptions<T> {
}
