package io.tarantool.driver.api.space;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

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
     * Inserts tuple into the space, if no tuple with same unique keys exists. Otherwise throw duplicate key error.
     *
     * @param tuple new data
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> insert(TarantoolTuple tuple) throws TarantoolClientException;

    /**
     * Insert a tuple into the space or replace an existing one.
     *
     * @param tuple new data
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> replace(TarantoolTuple tuple) throws TarantoolClientException;

    /**
     * Select tuples matching the specified query with options.
     *
     * @param conditions query with options
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> select(Conditions conditions) throws TarantoolClientException;

    /**
     * Update a tuple
     *
     * @param conditions query with options
     * @param tuple tuple with new field values
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
     * Update tuple if it would be found elsewhere try to insert tuple. Always use primary index for key.
     *
     * @param conditions query with options
     * @param tuple new data that will be insert if tuple will be not found
     * @param operations the list of update operations to be performed if the tuple exists
     * @return a future that will empty list
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<TarantoolResult<TarantoolTuple>> upsert(Conditions conditions,
                                                              TarantoolTuple tuple,
                                                              TupleOperations operations);

    /**
     * Get metadata associated with this space
     *
     * @return space metadata
     */
    TarantoolSpaceMetadata getMetadata();
}
