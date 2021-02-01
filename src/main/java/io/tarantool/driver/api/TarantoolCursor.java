package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;

/**
 * Basic cursor interface which allows forward-only iteration
 * through query results.
 *
 * Warning: 'TarantoolCursor' cursors are not thread-safe.
 *
 * @author Vladimir Rogach
 */
public interface TarantoolCursor<T> {

    /**
     * Fetch next element.
     *
     * @return true if element was fetched, false if no elements left.
     * @throws TarantoolClientException if the request to server failed.
     */
    boolean next() throws TarantoolClientException;

    /**
     * @return current element
     * @throws TarantoolSpaceOperationException if cursor is invalid,
     *                                          for example {@link TarantoolCursor#next()} was not called.
     */
    T get() throws TarantoolSpaceOperationException;
}
