package io.tarantool.driver.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;

/**
 * Used in {@link SpacesMetadataProvider} for index metadata mapping
 *
 * @author Alexey Kuzin
 */
public interface TarantoolIndexMetadataResult extends SingleValueCallResult<TarantoolResult<TarantoolIndexMetadata>> {
}
