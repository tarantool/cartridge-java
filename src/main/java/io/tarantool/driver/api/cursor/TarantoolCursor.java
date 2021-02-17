package io.tarantool.driver.api.cursor;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;

/**
 * Basic cursor interface which allows forward-only iteration
 * through query results.
 *
 * Warning: 'TarantoolCursor' cursors are not thread-safe.
 *
 * @author Vladimir Rogach
 */
public interface TarantoolCursor<T extends Packable> {

    /**
     * Fetch next element.
     *
     * @return true if element was fetched, false if no elements left.
     * @throws TarantoolClientException if the request to server failed.
     */
    boolean next() throws TarantoolClientException;

    /**
     * @return current element or null if cursor is not initialized
     * @throws TarantoolSpaceOperationException when no data is available
     */
    T get() throws TarantoolSpaceOperationException;
}
