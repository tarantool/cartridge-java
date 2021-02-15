package io.tarantool.driver.metadata;

import io.tarantool.driver.api.SingleValueCallResult;

/**
 * Shortcut for {@link SingleValueCallResult} with Tarantool metadata
 *
 * @author Alexey Kuzin
 */
public interface DDLMetadataContainerResult extends SingleValueCallResult<TarantoolMetadataContainer> {
}
