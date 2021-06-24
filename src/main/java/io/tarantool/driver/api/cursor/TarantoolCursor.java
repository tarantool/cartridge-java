package io.tarantool.driver.api.cursor;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.Iterator;

/**
 * Basic cursor interface which allows forward-only iteration
 * through query results.
 *
 * Warning: 'TarantoolCursor' cursors are not thread-safe.
 *
 * @author Vladimir Rogach
 */
public interface TarantoolCursor<T extends Packable> extends Iterator<T> {
}
