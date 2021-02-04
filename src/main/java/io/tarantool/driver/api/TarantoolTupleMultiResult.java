package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.TarantoolResultConverter;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Shortcut for {@link MultiValueCallResult} with default tuple result
 *
 * @author Alexey Kuzin
 * @see TarantoolTupleResult
 */
public class TarantoolTupleMultiResult
        extends MultiValueCallResultImpl<TarantoolTuple, TarantoolResult<TarantoolTuple>> {
    /**
     * basic constructor
     *
     * @param result function call result (first item)
     * @param tupleConverter converter for tuples in result
     */
    public TarantoolTupleMultiResult(ArrayValue result, ValueConverter<ArrayValue, TarantoolTuple> tupleConverter) {
        super(result, new TarantoolResultConverter<>(tupleConverter));
    }
}
