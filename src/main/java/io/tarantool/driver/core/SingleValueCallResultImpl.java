package io.tarantool.driver.core;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.exceptions.errors.TarantoolErrorsParser;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.function.Function;

/**
 * Basic {@link SingleValueCallResult} implementation. If the result array contains two values where the first is
 * {@code null}, the second is treated as a formatted error or an error message.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class SingleValueCallResultImpl<T> implements SingleValueCallResult<T> {

    private final T value;

    public SingleValueCallResultImpl(ArrayValue result, ValueConverter<Value, T> valueConverter) {
        value = parseResult(result, valueConverter::fromValue);
    }

    public SingleValueCallResultImpl(ArrayValue result, MessagePackValueMapper valueMapper) {
        value = parseResult(result, valueMapper::fromValue);
    }

    private T parseResult(ArrayValue result, Function<Value, T> valueGetter) {
        if (result == null) {
            throw new TarantoolFunctionCallException("Function call result is null");
        }
        if (result.size() == 0 || result.size() == 1 && result.get(0).isNilValue()) {
            // [nil] or []
            return null;
        } else if (result.size() == 2 && (result.get(0).isNilValue() && !result.get(1).isNilValue())) {
            // [nil, "Error msg..."] or [nil, {str="Error msg...", stack="..."}]
            throw TarantoolErrorsParser.parse(result.get(1));
        } else {
            // [result]
            return valueGetter.apply(result.get(0));
        }
    }

    @Override
    public T value() {
        return value;
    }
}
