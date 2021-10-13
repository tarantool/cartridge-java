package io.tarantool.driver.api.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.core.metadata.SpacesMetadataProvider;
import io.tarantool.driver.core.metadata.TarantoolIndexMetadata;

/**
 * Used in {@link SpacesMetadataProvider} for index metadata mapping
 *
 * @author Alexey Kuzin
 */
public interface TarantoolIndexMetadataResult extends SingleValueCallResult<TarantoolResult<TarantoolIndexMetadata>> {
}
