package io.tarantool.driver.mappers;

import java.util.List;

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
}
