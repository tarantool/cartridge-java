package io.tarantool.driver.api.cursor;

import java.io.Closeable;

/**
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public interface TarantoolCursor<T> extends TarantoolIterator<T>, TarantoolIterable<T>, Closeable {

}
