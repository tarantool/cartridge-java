package io.tarantool.driver.api.space;

import io.tarantool.driver.api.TarantoolCursor;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
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
     * @param conditions query with options
     * @return a future that will contain removed tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> delete(Conditions conditions)
            throws TarantoolClientException;

    /**
     * Delete a tuple
     *
     * @param conditions     query with options
     * @param tupleConverter the entity-to-object tupleConverter capable of converting MessagePack {@link ArrayValue}
     *  into an object of type {@code T}
     * @param <T>            result type
     * @return a future that will contain removed tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> delete(Conditions conditions,
                                                     ValueConverter<ArrayValue, T> tupleConverter)
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
     * @param tuple          new data
     * @param tupleConverter the entity-to-object tupleConverter capable of converting MessagePack {@link ArrayValue}
     *   into an object of type {@code T}
     * @param <T>            result tuple type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple, ValueConverter<ArrayValue, T> tupleConverter)
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
     * @param tuple          new data
     * @param tupleConverter the entity-to-object tupleConverter capable of converting MessagePack {@link ArrayValue}
     *                      into
     *                       an object of type {@code T}
     * @param <T>            result tuple type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> replace(TarantoolTuple tuple,
                                                      ValueConverter<ArrayValue, T> tupleConverter)
            throws TarantoolClientException;

    /**
     * Select tuples matching the specified query with options.
     *
     * @param conditions query with options
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> select(Conditions conditions) throws TarantoolClientException;

    /**
     * Select tuples matching the specified index query with options.
     *
     * @param conditions query with options
     * @param tupleClass target tuple class
     * @param <T>        target tuple type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed or value converter not found
     */
    <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                     Class<T> tupleClass) throws TarantoolClientException;

    /**
     * Select tuples matching the specified query with options.
     *
     * @param conditions     query with options
     * @param tupleConverter the entity-to-object tupleConverter capable of converting MessagePack {@link ArrayValue}
     *  into an object of type {@code T}
     * @param <T>            target tuple type
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                     ValueConverter<ArrayValue, T> tupleConverter)
            throws TarantoolClientException;

    /**
     * Update a tuple
     *
     * @param conditions query with options
     * @param tuple      tuple with new field values
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> update(Conditions conditions, TarantoolTuple tuple);

    /**
     * Update a tuple
     *
     * @param conditions query with options
     * @param operations the list update operations
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> update(Conditions conditions, TupleOperations operations);

    /**
     * Update a tuple
     *
     * @param conditions     query with options
     * @param operations     the list update operations
     * @param tupleConverter the entity-to-object tupleConverter capable of converting MessagePack {@link ArrayValue}
     *                      into
     *                       an object of type {@code T}
     * @param <T>            result type
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> update(Conditions conditions,
                                                     TupleOperations operations,
                                                     ValueConverter<ArrayValue, T> tupleConverter);

    /**
     * Update tuple if it would be found elsewhere try to insert tuple. Always use primary index for key.
     *
     * @param conditions query with options
     * @param tuple      new data that will be insert if tuple will be not found
     * @param operations the list of update operations to be performed if the tuple exists
     * @return a future that will empty list
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> upsert(Conditions conditions,
                                                              TarantoolTuple tuple,
                                                              TupleOperations operations);

    /**
     * Update tuple if it would be found elsewhere try to insert tuple. Always use primary index for key.
     *
     * @param conditions     query with options
     * @param tuple          new data that will be insert if tuple will be not found
     * @param operations     the list of update operations to be performed if the tuple exists
     * @param tupleConverter the entity-to-object tupleConverter capable of converting MessagePack {@link ArrayValue}
     *                      into
     *                       an object of type {@code T}
     * @param <T>            result type
     * @return a future that will empty list
     * @throws TarantoolClientException in case if the request failed
     */
    <T> CompletableFuture<TarantoolResult<T>> upsert(Conditions conditions,
                                                     TarantoolTuple tuple,
                                                     TupleOperations operations,
                                                     ValueConverter<ArrayValue, T> tupleConverter);

    /**
     * Get metadata associated with this space
     *
     * @return space metadata
     */
    TarantoolSpaceMetadata getMetadata();

    /**
     * Perform a batch 'select' request.
     * Select will fetch tuples matching the specified query.
     * Each request to server will fetch no more than 'batch size' tuples.
     *
     * @param conditions query with options
     * @param batchSize  size of a batch of single client request
     * @return a cursor that will contain all corresponding tuples
     */
    TarantoolCursor<TarantoolTuple> cursor(Conditions conditions, int batchSize);

    /**
     * Perform a batch 'select' request.
     * See {@link TarantoolSpaceOperations#cursor(Conditions, int)}
     *
     * @param conditions query with options
     * @param batchSize  size of a batch of single client request
     * @param tupleClass target tuple class
     * @param <T>        target tuple type
     * @return a cursor that will contain all corresponding tuples
     */
    <T> TarantoolCursor<T> cursor(Conditions conditions, int batchSize, Class<T> tupleClass);

}
