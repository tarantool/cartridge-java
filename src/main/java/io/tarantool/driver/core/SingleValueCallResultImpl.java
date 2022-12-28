package io.tarantool.driver.core;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.exceptions.errors.TarantoolErrorsParser;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;
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
        int callResultSize = result.size();
        Value resultValue = result.getOrNilValue(0);
        Value errorsValue = result.getOrNilValue(1);
        if (callResultSize == 0 || callResultSize == 1 && resultValue.isNilValue()) {
            // [nil] or []
            return null;
        } else if (callResultSize == 2 && (resultValue.isNilValue() && !errorsValue.isNilValue())) {
            // [nil, "Error msg..."] or [nil, {str="Error msg...", stack="..."}]
            throw TarantoolErrorsParser.parse(errorsValue);
        } else if (callResultSize == 2 && errorsValue.isArrayValue()) {
            // [result, errors]
            List<Value> errorsList = errorsValue.asArrayValue().list();
            // We are not really interested in all errors, since the operation is already failed
            if (!errorsList.isEmpty()) {
                throw TarantoolErrorsParser.parse(errorsList.get(0));
            }
            throw new TarantoolFunctionCallException("Unexpected error format in the function call result");
        } else if (callResultSize > 1 && !errorsValue.isNilValue()) {
            throw new TarantoolFunctionCallException(
                "Too many values in the function call result, " +
                    "expected \"[result]\", \"[result, errors]\" or \"[nil, error]\""
            );
        } else {
            // [result]
            return valueGetter.apply(resultValue);
        }
    }

    @Override
    public T value() {
        return value;
    }
}
