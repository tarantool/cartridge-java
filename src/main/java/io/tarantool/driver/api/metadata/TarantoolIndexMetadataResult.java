package io.tarantool.driver.api.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;

/**
 * Shortcut for {@link SingleValueCallResult}, that contains index metadata mapping
 *
 * @author Alexey Kuzin
 */
public interface TarantoolIndexMetadataResult extends SingleValueCallResult<TarantoolResult<TarantoolIndexMetadata>> {
}
