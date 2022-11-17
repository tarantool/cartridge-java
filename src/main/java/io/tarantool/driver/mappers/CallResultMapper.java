package io.tarantool.driver.mappers;

import io.tarantool.driver.api.CallResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.ValueConverterWithInputTypeWrapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;

/**
 * Special tuple mapper for API function call results.
 * <p>
 * The result is always an array since Lua <code>return</code> is a multi-return, and if the first value is
 * <code>nil</code>, the second non-null value is interpreted as an error object or error message.
 *
 * @param <T> target tuple type
 * @param <R> target result type
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public class CallResultMapper<T, R extends CallResult<T>> extends AbstractResultMapper<R> {

    /**
     * Basic constructor
     *
     * @param valueMapper     value mapper to be used for the multi-return result
     * @param resultConverter MessagePack result array to call result converter
     * @param resultClass     tuple result class
     */
    public CallResultMapper(
        MessagePackValueMapper valueMapper, ValueConverter<? extends Value, ? extends R> resultConverter,
        Class<? extends R> resultClass) {
        super(valueMapper, resultConverter, resultClass);
    }

    public CallResultMapper(
        MessagePackValueMapper valueMapper, ValueConverter<? extends Value, ? extends R> resultConverter) {
        super(valueMapper, resultConverter);
    }

    public CallResultMapper(
        MessagePackValueMapper valueMapper, ValueType valueType,
        ValueConverter<? extends Value, ? extends R> resultConverter, Class<? extends R> resultClass) {
        super(valueMapper, valueType, resultConverter, resultClass);
    }

    public CallResultMapper(
        MessagePackValueMapper valueMapper, ValueType valueType,
        ValueConverter<? extends Value, ? extends R> resultConverter) {
        super(valueMapper, valueType, resultConverter);
    }

    public CallResultMapper(
        MessagePackValueMapper valueMapper, List<ValueConverterWithInputTypeWrapper<R>> converters,
        Class<? extends R> resultClass) {
        super(valueMapper, converters, resultClass);
    }

    public CallResultMapper(
        MessagePackValueMapper valueMapper, List<ValueConverterWithInputTypeWrapper<R>> converters) {
        super(valueMapper, converters);
    }
}
