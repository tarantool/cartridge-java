package io.tarantool.driver.api.space.options.crud;

import java.util.Optional;

import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

public interface OperationWithFullScanOptions<T extends OperationWithModeOptions<T>> extends Options, Self<T> {

    /**
     * Sets "fullscan" option value to true. If true then a critical log entry will be skipped on potentially long
     * select.
     *
     * @return this options instance.
     */
    default T fullScan() {
        addOption(ProxyOption.FULL_SCAN, true);
        return self();
    }

    /**
     * @return "fullscan" option value.
     */
    default Optional<Boolean> getFullScan() {
        return getOption(ProxyOption.FULL_SCAN, Boolean.class);
    }
}
