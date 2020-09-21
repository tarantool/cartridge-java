package io.tarantool.driver.api.space;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.operations.TupleOperations;
import org.msgpack.value.ArrayValue;

import java.util.concurrent.CompletableFuture;

/**
 * Tarantool space operations interface (create, insert, replace, delete...)
 *
 * @author Alexey Kuzin
 */
public interface TarantoolSpaceOperations {

    /**
     * Delete a tuple
     *
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                         values for matching
     * @return a future that will contain removed tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> delete(TarantoolIndexQuery indexQuery)
            throws TarantoolClientException;

    /**
     * Delete a tuple
     *
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                         values for matching
     * @param tupleMapper the entity-to-object tupleMapper capable of converting MessagePack {@link ArrayValue} into
     *                          an object of type {@code T}
     * @param <T> result type
     * @return a future that will contain removed tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> delete(TarantoolIndexQuery indexQuery,
                                                     ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException;

    /**
     * Inserts tuple into the space, if no tuple with same unique keys exists. Otherwise throw duplicate key error.
     *
     * @param tuple new data
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> insert(TarantoolTuple tuple) throws TarantoolClientException;

    /**
     * Inserts tuple into the space, if no tuple with same unique keys exists. Otherwise throw duplicate key error.
     *
     * @param tuple new data
     * @param tupleMapper the entity-to-object tupleMapper capable of converting MessagePack {@link ArrayValue} into
     *                    an object of type {@code T}
     * @param <T> result tuple type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple, ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException;

    /**
     * Insert a tuple into the space or replace an existing one.
     *
     * @param tuple new data
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> replace(TarantoolTuple tuple) throws TarantoolClientException;

    /**
     * Insert a tuple into the space or replace an existing one.
     *
     * @param tuple new data
     * @param tupleMapper the entity-to-object tupleMapper capable of converting MessagePack {@link ArrayValue} into
     *                    an object of type {@code T}
     * @param <T> result tuple type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> replace(TarantoolTuple tuple, ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException;

    /**
     * Select all tuples using the default (primary) index.
     * Warning: this operation can result in significant amount of data transferred over network and saved in
     * the client host memory. Do not forget setting the limit of retrieved tuples on large spaces!
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolSelectOptions options)
            throws TarantoolClientException;

    /**
     * Select all tuples using the index specified by name.
     * Warning: this operation can result in significant amount of data transferred over network and saved in
     * the client host memory. Do not forget setting the limit of retrieved tuples on large spaces!
     * @param indexName the index name, must not be empty or null
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName, TarantoolSelectOptions options)
            throws TarantoolClientException;

    /**
     * Select all tuples using the index specified by name with the specified iterator type.
     * Warning: this operation can result in significant amount of data transferred over network and saved in
     * the client host memory. Do not forget setting the limit of retrieved tuples on large spaces!
     * @param indexName the index name, must not be empty or null
     * @param iteratorType iterator type (EQ, REQ, etc.)
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName, TarantoolIteratorType iteratorType,
                                                              TarantoolSelectOptions options)
            throws TarantoolClientException;

    /**
     * Select tuples matching the specified index query.
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                   values for matching
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolIndexQuery indexQuery,
                                                              TarantoolSelectOptions options)
            throws TarantoolClientException;

    /**
     * Select tuples matching the specified index query.
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                   values for matching
     * @param options query options such as offset and limit
     * @param clazz result type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed or value converter not found
     */
    <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                     TarantoolSelectOptions options,
                                                     Class<T> clazz) throws TarantoolClientException;

    /**
     * Select tuples matching the specified index query.
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                   values for matching
     * @param options query options such as offset and limit
     * @param tupleMapper the entity-to-object tupleMapper capable of converting MessagePack {@link ArrayValue} into
     *                    an object of type {@code T}
     * @param <T> result tuple type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery, TarantoolSelectOptions options,
                                                     ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException;

    /**
     * Update a tuple
     *
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                   values for matching
     * @param operations the list update operations
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> update(TarantoolIndexQuery indexQuery,
                                                              TupleOperations operations);

    /**
     * Update a tuple
     *
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                   values for matching
     * @param operations the list update operations
     * @param tupleMapper the entity-to-object tupleMapper capable of converting MessagePack {@link ArrayValue} into
     *                    an object of type {@code T}
     * @param <T> result type
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> update(TarantoolIndexQuery indexQuery,
                                                     TupleOperations operations,
                                                     ValueConverter<ArrayValue, T> tupleMapper);

    /**
     * Update tuple if it would be found elsewhere try to insert tuple. Always use primary index for key.
     *
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                         values for matching
     * @param tuple new data that will be insert if tuple will be not found
     * @param operations the list of update operations to be performed if the tuple exists
     * @return a future that will empty list
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> upsert(TarantoolIndexQuery indexQuery,
                                                              TarantoolTuple tuple,
                                                              TupleOperations operations);

    /**
     * Update tuple if it would be found elsewhere try to insert tuple. Always use primary index for key.
     *
     * @param indexQuery the index query, containing information about the used index, iterator type and index key
     *                         values for matching
     * @param tuple new data that will be insert if tuple will be not found
     * @param operations the list of update operations to be performed if the tuple exists
     * @param tupleMapper the entity-to-object tupleMapper capable of converting MessagePack {@link ArrayValue} into
     *                          an object of type {@code T}
     * @param <T> result type
     * @return a future that will empty list
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> upsert(TarantoolIndexQuery indexQuery,
                                                     TarantoolTuple tuple,
                                                     TupleOperations operations,
                                                     ValueConverter<ArrayValue, T> tupleMapper);
}
