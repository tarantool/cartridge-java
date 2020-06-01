package io.tarantool.driver.space;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.TarantoolProtocolException;

import java.util.concurrent.CompletableFuture;

/**
 * Tarantool space operations interface (create, insert, replace, delete...)
 *
 * @author Alexey Kuzin
 */
public interface TarantoolSpaceOperations {
    /**
     * Select all tuples using the default (primary) index.
     * Warning: this operation can result in significant amount of data transferred over network and saved in
     * the client host memory. Do not forget setting the limit of retrieved tuples on large spaces!
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     */
    CompletableFuture<TarantoolResult> select(TarantoolSelectOptions options) throws TarantoolProtocolException;

    /**
     * Select all tuples using the index specified by name.
     * Warning: this operation can result in significant amount of data transferred over network and saved in
     * the client host memory. Do not forget setting the limit of retrieved tuples on large spaces!
     * @param indexName the index name, must not be empty or null
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     */
    CompletableFuture<TarantoolResult> select(String indexName, TarantoolSelectOptions options) throws TarantoolProtocolException;

    /**
     * Select all tuples using the index specified by name with the specified iterator type.
     * Warning: this operation can result in significant amount of data transferred over network and saved in
     * the client host memory. Do not forget setting the limit of retrieved tuples on large spaces!
     * @param indexName the index name, must not be empty or null
     * @param iteratorType iterator type (EQ, REQ, etc.)
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     */
    CompletableFuture<TarantoolResult> select(String indexName, TarantoolIteratorType iteratorType, TarantoolSelectOptions options) throws TarantoolProtocolException;

    /**
     * Select tuples matching the specified index query.
     * @param indexQuery the index query, containing information about the used index, iterator type and index key values for matching
     * @param options query options such as offset and limit
     * @return a future that will contain all corresponding tuples once completed
     */
    CompletableFuture<TarantoolResult> select(TarantoolIndexQuery indexQuery, TarantoolSelectOptions options) throws TarantoolProtocolException;

    CompletableFuture<TarantoolResult> update(); // TODO update parameters
    CompletableFuture<TarantoolResult> replace(); // TODO replace parameters
    CompletableFuture<TarantoolResult> delete(); // TODO delete parameters
}
