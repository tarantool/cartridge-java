package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;

/**
 * Default mapper for {@link MultiValueCallResult} with content types supported by the given value mapper
 *
 * @author Alexey Kuzin
 * @param <T> target result content type
 */
public class DefaultMultiValueResultMapper<T, R extends List<T>>
        extends CallResultMapper<R, MultiValueCallResult<T, R>> {

    /**
     * Basic constructor with value converter
     *
     * @param valueMapper value mapper for result content conversion
     * @param valueConverter MessagePack value to object converter for each item
     * @param contentClass target result content class
     */
    public DefaultMultiValueResultMapper(MessagePackMapper valueMapper,
                                         ValueConverter<Value, T> valueConverter,
                                         Class<T> contentClass) {
        super(DefaultMessagePackMapperFactory.getInstance().emptyMapper(),
                defaultValueConverter(valueConverter, contentClass), getResultClass(contentClass));
    }

    /**
     * Basic constructor
     *
     * @param valueMapper value mapper for result content conversion
     * @param contentClass target result content class
     */
    public DefaultMultiValueResultMapper(MessagePackMapper valueMapper, Class<T> contentClass) {
        super(DefaultMessagePackMapperFactory.getInstance().emptyMapper(),
                defaultValueConverter(valueMapper), getResultClass(contentClass));
    }

    @SuppressWarnings("unchecked")
    private static <T, R extends  List<T>> Class<MultiValueCallResult<T, R>> getResultClass(Class<T> contentClass) {
        return (Class<MultiValueCallResult<T, R>>) (Class<?>) MultiValueCallResult.class;
    }

    private static
    <T, R extends List<T>> ValueConverter<ArrayValue, ? extends MultiValueCallResult<T, R>>
    defaultValueConverter(ValueConverter<Value, T> valueConverter,
                          Class<T> contentClass) {
        MessagePackValueMapper valueMapper = DefaultMessagePackMapperFactory.getInstance().emptyMapper();
        valueMapper.registerValueConverter(Value.class, contentClass, valueConverter);
        return new MultiValueCallResultConverter<>(valueMapper::fromValue);
    }

    private static
    <T, R extends List<T>> ValueConverter<ArrayValue, ? extends MultiValueCallResult<T, R>>
    defaultValueConverter(MessagePackValueMapper valueMapper) {
        return new MultiValueCallResultConverter<>(valueMapper::fromValue);
    }
}
