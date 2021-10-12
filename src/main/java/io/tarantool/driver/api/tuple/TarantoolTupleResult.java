package io.tarantool.driver.api.tuple;

import io.tarantool.driver.api.TarantoolResult;

/**
 * Shortcut for {@link TarantoolResult} with default tuples
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTupleResult extends TarantoolResult<TarantoolTuple> {
}
