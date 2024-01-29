package io.tarantool.driver.api.space.options.crud;

import java.util.Optional;

import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

/**
 * Base interface for all operation options that may have a configurable "fetch_latest_metadata" option.
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithFetchLatestMetadataOptions<T extends OperationWithFetchLatestMetadataOptions<T>>
    extends Options, Self<T> {

    /**
     * Sets "fetch_latest_metadata" option value to true. Guarantees the up-to-date metadata (space format) in first
     * return value, otherwise it may not take into account the latest migration of the data format. Performance
     * overhead is up to 15%. False by default.
     * @return this options instance.
     */
    default T fetchLatestMetadata() {
        addOption(ProxyOption.FETCH_LATEST_METADATA, true);
        return self();
    }

    /**
     * @return "fetch_latest_metadata" option value.
     */
    default Optional<Boolean> getFetchLatestMetadata() {
        return getOption(ProxyOption.FETCH_LATEST_METADATA, Boolean.class);
    }
}
