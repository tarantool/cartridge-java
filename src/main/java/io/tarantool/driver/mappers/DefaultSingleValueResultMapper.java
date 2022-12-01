package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.SingleValueCallResultConverter;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.msgpack.value.ArrayValue;

/**
 * Default mapper for {@link SingleValueCallResult} with content types supported by the given value mapper
 *
 * @param <T> target result content type
 * @author Alexey Kuzin
 */
public class DefaultSingleValueResultMapper<T> extends CallResultMapper<T, SingleValueCallResult<T>> {

    /**
     * Basic constructor
     *
     * @param valueMapper  value mapper for result content conversion
     * @param contentClass target result content class
     */
    public DefaultSingleValueResultMapper(MessagePackMapper valueMapper, Class<T> contentClass) {
        super(DefaultMessagePackMapperFactory.getInstance().emptyMapper(),
            defaultValueConverter(valueMapper), getResultClass(contentClass));
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
