package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;

/**
 * Shortcut for {@link TarantoolResult} with default tuples
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTupleResult extends TarantoolResult<TarantoolTuple> {
}
