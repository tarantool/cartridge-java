package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Manages instantiation of the operation result factories
 *
 * @author Alexey Kuzin
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

    public
    <T, R extends List<T>> CallResultMapper<R, MultiValueCallResult<T, R>>
    getMultiValueResultMapper(Supplier<R> containerSupplier, ValueConverter<Value, T> valueConverter) {
        return this.<T, R>multiValueResultMapperFactory()
                .withMultiValueResultConverter(new MultiValueListConverter<>(valueConverter, containerSupplier));
    }

    public
    <T> CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    getTarantoolResultMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return this.<T>singleValueTarantoolResultMapperFactory()
                .withTarantoolResultConverter(getConverter(mapper, ArrayValue.class, tupleClass));
    }

    public
    <T, R extends List<T>> CallResultMapper<R, MultiValueCallResult<T, R>>
    getDefaultMultiValueMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return new DefaultMultiValueResultMapper<>(mapper, tupleClass);
    }

    public
    <T> CallResultMapper<T, SingleValueCallResult<T>>
    getDefaultSingleValueMapper(MessagePackMapper mapper, Class<T> tupleClass) {
        return new DefaultSingleValueResultMapper<>(mapper, tupleClass);
    }

    private <V extends Value, T> ValueConverter<V, T> getConverter(
            MessagePackMapper mapper, Class<V> valueClass, Class<T> tupleClass) {
        Optional<? extends ValueConverter<V, T>> converter = mapper.getValueConverter(valueClass, tupleClass);
        if (!converter.isPresent()) {
            throw new TarantoolClientException(
                    "No converter for value class %s and type %s is present", valueClass, tupleClass);
        }
        return converter.get();
    }
}
