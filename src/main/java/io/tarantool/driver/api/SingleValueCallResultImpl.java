package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

/**
 * Basic {@link SingleValueCallResult} implementation
 *
 * @author Alexey Kuzin
 */
public class SingleValueCallResultImpl<T> implements SingleValueCallResult<T> {

    private T value;

    public SingleValueCallResultImpl(ArrayValue result, ValueConverter<Value, T> valueConverter) {
        if (result == null) {
            throw new TarantoolFunctionCallException("Function call result is null");
        }
        if (result.size() == 0 || result.size() == 1 && result.get(0).isNilValue()) {
            // [nil] or []
            value = null;
        } else if (result.size() == 2 && (result.get(0).isNilValue() && !result.get(1).isNilValue())) {
            // [nil, "Error msg..."] or [nil, {str="Error msg...", stack="..."}]
            if (result.get(1).isMapValue()) {
                // Probably a formatted error
                throw new TarantoolFunctionCallException(result.get(1).asMapValue());
            } else {
                throw new TarantoolFunctionCallException(result.get(1).toString());
            }
        } else {
            // [result]
            value = valueConverter.fromValue(result.get(0));
        }
    }

    @Override
    public T value() {
        return value;
    }
}
