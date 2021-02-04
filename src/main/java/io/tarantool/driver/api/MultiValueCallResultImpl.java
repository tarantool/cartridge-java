package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;

/**
 * Basic {@link MultiValueCallResult} implementation
 *
 * @author Alexey Kuzin
 */
public class MultiValueCallResultImpl<T, R extends List<T>> implements MultiValueCallResult<T, R> {

    private final R value;

    public MultiValueCallResultImpl(Value result, ValueConverter<ArrayValue, R> valueConverter) {
        if (result == null) {
            throw new TarantoolFunctionCallException("Function result is null");
        }
        if (!result.isArrayValue()) {
            throw new TarantoolFunctionCallException("Function result is not a MessagePack array");
        }
        this.value = valueConverter.fromValue(result.asArrayValue());
    }

    @Override
    public R value() {
        return value;
    }
}
