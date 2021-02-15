package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;

/**
 * Shortcut for {@link SingleValueCallResult} with default tuple result
 *
 * @author Alexey Kuzin
 * @see TarantoolTupleResult
 */
public interface TarantoolTupleSingleResult extends SingleValueCallResult<TarantoolResult<TarantoolTuple>> {
}
