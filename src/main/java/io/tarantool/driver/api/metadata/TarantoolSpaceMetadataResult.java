package io.tarantool.driver.api.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;

/**
 * Shortcut for {@link SingleValueCallResult}, that contains space metadata mapping.
 *
 * @author Alexey Kuzin
 */
public interface TarantoolSpaceMetadataResult extends SingleValueCallResult<TarantoolResult<TarantoolSpaceMetadata>> {
}
