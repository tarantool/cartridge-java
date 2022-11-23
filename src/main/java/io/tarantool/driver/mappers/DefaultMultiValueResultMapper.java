package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.MultiValueCallResultConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;

/**
 * Default mapper for {@link MultiValueCallResult} with content types supported by the given value mapper
 *
 * @param <T> target result content type
 * @author Alexey Kuzin
 */
public class DefaultMultiValueResultMapper<T, R extends List<T>>
    extends CallResultMapper<R, MultiValueCallResult<T, R>> {

    /**
     * Basic constructor
     *
     * @param valueMapper  value mapper for result content conversion
     * @param contentClass target result content class
     */
    public DefaultMultiValueResultMapper(MessagePackMapper valueMapper, Class<T> contentClass) {
        super(DefaultMessagePackMapperFactory.getInstance().emptyMapper(),
            defaultValueConverter(valueMapper), getResultClass(contentClass));
    }

    @SuppressWarnings("unchecked")
    private static <T, R extends List<T>> Class<MultiValueCallResult<T, R>> getResultClass(Class<T> contentClass) {
        return (Class<MultiValueCallResult<T, R>>) (Class<?>) MultiValueCallResult.class;
    }

    private static <T, R extends List<T>> ValueConverter<ArrayValue, ? extends MultiValueCallResult<T, R>>
    defaultValueConverter(MessagePackValueMapper valueMapper) {
        return new MultiValueCallResultConverter<>(valueMapper::fromValue);
    }
}
