package io.tarantool.driver.api.tuple;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.TarantoolResult;

/**
 * Shortcut for {@link MultiValueCallResult} with default tuple result
 *
 * @author Alexey Kuzin
 * @see TarantoolTupleResult
 */
public interface MultiValueTarantoolTupleResult
    extends MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>> {
}
