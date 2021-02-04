package io.tarantool.driver.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;

/**
 * Used in {@link SpacesMetadataProvider} for space metadata mapping
 *
 * @author Alexey Kuzin
 */
public interface TarantoolSpaceMetadataResult extends SingleValueCallResult<TarantoolResult<TarantoolSpaceMetadata>> {
}
