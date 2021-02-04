package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;

import java.util.List;

/**
 * Provides different factories for creating result mappers
 *
 * @author Alexey Kuzin
 */
public interface ResultMapperFactoryFactory {
    /**
     * Default factory for call result with a list of tuples.
     * Use this factory for handling default standalone server protocol results.
     *
     * @return default factory for array of tuple results
     */
    TarantoolTupleResultMapperFactory defaultTupleResultMapperFactory();

    /**
     * Default factory for single value stored function call result with a list of tuples.
     * Use this factory for handling default proxy function call results like tarantool/crud module API.
     *
     * @return default factory for single value call result with a list of tuples
     */
    TarantoolTupleSingleResultMapperFactory defaultTupleSingleResultMapperFactory();

    /**
     * Default factory for multi value stored function call result, where each return item is a tuple.
     * Use this factory for handling proxy function call results which return tuples as a multi-return result.
     *
     * @return default factory for multi value call result, where each item is a tuple
     */
    TarantoolTupleMultiResultMapperFactory defaultTupleMultiResultMapperFactory();

    /**
     * Create a factory for mapping Tarantool server protocol result to a list of tuples as {@link TarantoolResult}
     *
     * @param <T> target tuple type
     * @return new or existing factory instance
     */
    <T> TupleResultMapperFactory<T> tupleResultMapperFactory();

    /**
     * Create a factory for mapping Tarantool server protocol result to a list of tuples as {@link TarantoolResult}
     *
     * @param <T> target tuple type
     * @param tupleClass target tuple class
     * @return new or existing factory instance
     */
    <T> TupleResultMapperFactory<T> tupleResultMapperFactory(Class<T> tupleClass);

    /**
     * Create a factory for mapping stored function call results to {@link SingleValueCallResult}
     *
     * @param <T> target result type
     * @return new or existing factory instance
     */
    <T> SingleValueResultMapperFactory<T> singleValueResultMapperFactory();

    /**
     * Create a factory for mapping stored function call results to {@link SingleValueCallResult}
     *
     * @param <T> target result type
     * @param resultClass target result class
     * @return new or existing factory instance
     */
    <T> SingleValueResultMapperFactory<T> singleValueResultMapperFactory(Class<T> resultClass);

    /**
     * Create a factory for mapping stored function call result to {@link SingleValueCallResult} containing a list
     * of tuples mapped to {@link TarantoolResult}
     *
     * @param <T> target tuple type
     * @return new or existing factory instance
     */
    <T> SingleValueTarantoolResultMapperFactory<T> singleValueTarantoolResultMapperFactory();

    /**
     * Create a factory for mapping stored function call result to {@link SingleValueCallResult} containing a list
     * of tuples mapped to {@link TarantoolResult}
     *
     * @param <T> target tuple type
     * @param tupleClass target tuple class
     * @return new or existing factory instance
     */
    <T> SingleValueTarantoolResultMapperFactory<T> singleValueTarantoolResultMapperFactory(Class<T> tupleClass);

    /**
     * Create a factory for mapping stored function call results to {@link MultiValueCallResult}
     *
     * @param <T> target result content type
     * @param <R> target result type
     * @return new or existing factory instance
     */
    <T, R extends List<T>> MultiValueResultMapperFactory<T, R> multiValueResultMapperFactory();

    /**
     * Create a factory for mapping stored function call results to {@link MultiValueCallResult}
     *
     * @param <T> target result type
     * @param <R> target result type
     * @param resultClass target result class
     * @return new or existing factory instance
     */
    <T, R extends List<T>> MultiValueResultMapperFactory<T, R> multiValueResultMapperFactory(Class<R> resultClass);

    /**
     * Create a factory for mapping stored function call result to {@link MultiValueCallResult} containing a list
     * of tuples mapped to {@link TarantoolResult}
     *
     * @param <T> target tuple type
     * @return new or existing factory instance
     */
    <T> MultiValueTarantoolResultMapperFactory<T> multiValueTarantoolResultMapperFactory();

    /**
     * Create a factory for mapping stored function call result to {@link MultiValueCallResult} containing a list
     * of tuples mapped to {@link TarantoolResult}
     *
     * @param <T> target tuple type
     * @param tupleClass target tuple class
     * @return new or existing factory instance
     */
    <T> MultiValueTarantoolResultMapperFactory<T> multiValueTarantoolResultMapperFactory(
            Class<? extends TarantoolResult<T>> tupleClass);
}
