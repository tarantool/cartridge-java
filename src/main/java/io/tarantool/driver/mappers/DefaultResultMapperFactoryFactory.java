package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.custom.MultiValueListConverter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Manages instantiation of the operation result factories
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class DefaultResultMapperFactoryFactory implements ResultMapperFactoryFactory {

    /**
     * Basic constructor.
     */
    public DefaultResultMapperFactoryFactory() {
    }

    @Override
    public TarantoolTupleResultMapperFactory defaultTupleResultMapperFactory() {
        return new TarantoolTupleResultMapperFactory();
    }

    @Override
    public TarantoolTupleSingleResultMapperFactory defaultTupleSingleResultMapperFactory() {
        return new TarantoolTupleSingleResultMapperFactory();
    }

    @Override
    public TarantoolTupleMultiResultMapperFactory defaultTupleMultiResultMapperFactory() {
        return new TarantoolTupleMultiResultMapperFactory();
    }

    @Override
    public <T> TupleResultMapperFactory<T> tupleResultMapperFactory() {
        return new TupleResultMapperFactory<>();
    }

    @Override
    public <T> SingleValueResultMapperFactory<T> singleValueResultMapperFactory() {
        return new SingleValueResultMapperFactory<>();
    }

    @Override
    public <T> SingleValueTarantoolResultMapperFactory<T> singleValueTarantoolResultMapperFactory() {
        return new SingleValueTarantoolResultMapperFactory<>();
    }

    @Override
    public <T, R extends List<T>> MultiValueResultMapperFactory<T, R> multiValueResultMapperFactory() {
        return new MultiValueResultMapperFactory<>();
    }

    @Override
    public <T> MultiValueTarantoolResultMapperFactory<T> multiValueTarantoolResultMapperFactory() {
        return new MultiValueTarantoolResultMapperFactory<>();
    }

    public <T> CallResultMapper<T, SingleValueCallResult<T>>
    getSingleValueResultMapper(ValueConverter<Value, T> valueConverter) {
        return this.<T>singleValueResultMapperFactory().withSingleValueResultConverter(
            valueConverter, (Class<SingleValueCallResult<T>>) (Class<?>) SingleValueCallResult.class);
    }

    public <T, R extends List<T>> CallResultMapper<R, MultiValueCallResult<T, R>>
    getMultiValueResultMapper(Supplier<R> containerSupplier, ValueConverter<Value, T> valueConverter) {
        return this.<T, R>multiValueResultMapperFactory().withMultiValueResultConverter(
            new MultiValueListConverter<>(valueConverter, containerSupplier),
            (Class<MultiValueCallResult<T, R>>) (Class<?>) MultiValueCallResult.class);
    }

    public <T> CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    getTarantoolResultMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return this.<T>singleValueTarantoolResultMapperFactory()
            .withTarantoolResultConverter(getConverter(mapper, ValueType.ARRAY, tupleClass));
    }

    public <T, R extends List<T>> CallResultMapper<R, MultiValueCallResult<T, R>>
    getDefaultMultiValueMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return new DefaultMultiValueResultMapper<>(mapper, tupleClass);
    }

    public <T> CallResultMapper<T, SingleValueCallResult<T>>
    getDefaultSingleValueMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return new DefaultSingleValueResultMapper<>(mapper, tupleClass);
    }

    private <V extends Value, T> ValueConverter<V, T> getConverter(
        MessagePackMapper mapper, ValueType valueType, Class<T> tupleClass) {
        Optional<? extends ValueConverter<V, T>> converter = mapper.getValueConverter(valueType, tupleClass);
        if (!converter.isPresent()) {
            throw new TarantoolClientException(
                "No converter for value type %s and type %s is present", valueType, tupleClass);
        }
        return converter.get();
    }
}
