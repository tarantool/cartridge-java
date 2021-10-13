package io.tarantool.driver.api.metadata;

import io.tarantool.driver.api.SingleValueCallResult;

/**
 * Shortcut for {@link SingleValueCallResult} with Tarantool metadata
 *
 * @author Alexey Kuzin
 */
public interface DDLMetadataContainerResult extends SingleValueCallResult<TarantoolMetadataContainer> {
}
