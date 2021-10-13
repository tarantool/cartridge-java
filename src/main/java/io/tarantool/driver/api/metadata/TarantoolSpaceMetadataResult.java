package io.tarantool.driver.api.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.core.metadata.SpacesMetadataProvider;

/**
 * Used in {@link SpacesMetadataProvider} for space metadata mapping
 *
 * @author Alexey Kuzin
 */
public interface TarantoolSpaceMetadataResult extends SingleValueCallResult<TarantoolResult<TarantoolSpaceMetadata>> {
}
