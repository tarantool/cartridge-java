package io.tarantool.driver.api.tuple;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;

/**
 * Shortcut for {@link SingleValueCallResult} with default tuple result
 *
 * @author Alexey Kuzin
 * @see TarantoolTupleResult
 */
public interface TarantoolTupleSingleResult extends SingleValueCallResult<TarantoolResult<TarantoolTuple>> {
}
