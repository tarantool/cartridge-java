package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;

/**
 * Shortcut for {@link MultiValueCallResult} with default tuple result
 *
 * @author Alexey Kuzin
 * @see TarantoolTupleResult
 */
public interface TarantoolTupleMultiResult
        extends MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>> {
}
