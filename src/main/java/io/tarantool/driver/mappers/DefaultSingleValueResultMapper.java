package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;
import org.msgpack.value.ArrayValue;

/**
 * Default mapper for {@link SingleValueCallResult} with content types supported by the given value mapper
 *
 * @author Alexey Kuzin
 * @param <T> target result content type
 */
public class DefaultSingleValueResultMapper<T> extends CallResultMapper<T, SingleValueCallResult<T>> {

    /**
     * Constructor. The converter target type is determined via reflection.
     *
     * @param valueMapper value mapper for result content conversion
     * @param contentClass target result content class
     */
    public DefaultSingleValueResultMapper(MessagePackValueMapper valueMapper, Class<T> contentClass) {
        super(valueMapper, defaultValueConverter(valueMapper), getResultClass(contentClass));
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<SingleValueCallResult<T>> getResultClass(Class<T> contentClass) {
        return (Class<SingleValueCallResult<T>>) (Class<?>) SingleValueCallResult.class;
    }

    private static <T> ValueConverter<ArrayValue, ? extends SingleValueCallResult<T>> defaultValueConverter(
            MessagePackValueMapper valueMapper) {
        return new SingleValueCallResultConverter<>(valueMapper::fromValue);
    }
}
