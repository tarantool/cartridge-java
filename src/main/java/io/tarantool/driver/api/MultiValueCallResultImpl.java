package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.exceptions.errors.TarantoolErrorsParser;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;

/**
 * Basic {@link MultiValueCallResult} implementation. If the result array contains two values where the first is
 * {@code null}, the second is treated as a formatted error or an error message.
 *
 * @author Alexey Kuzin
 */
public class MultiValueCallResultImpl<T, R extends List<T>> implements MultiValueCallResult<T, R> {

    private final R value;

    public MultiValueCallResultImpl(Value result, ValueConverter<ArrayValue, R> valueConverter) {
        if (result == null) {
            throw new TarantoolFunctionCallException("Function call result is null");
        }
        if (!result.isArrayValue()) {
            throw new TarantoolFunctionCallException("Function call result is not a MessagePack array");
        }
        ArrayValue resultArray = result.asArrayValue();
        if (resultArray.size() == 2 && (resultArray.get(0).isNilValue() && !resultArray.get(1).isNilValue())) {
            // [nil, "Error msg..."] or [nil, {str="Error msg...", stack="..."}]
            throw TarantoolErrorsParser.parse(resultArray.get(1));
        } else {
            // result
            this.value = valueConverter.fromValue(result.asArrayValue());
        }
    }

    @Override
    public R value() {
        return value;
    }
}
