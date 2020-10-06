package io.tarantool.driver.api.cursor;

/**
 * @author Sergey Volgin
 */
public interface TarantoolIterable<T> extends Iterable<T> {

    @Override
    TarantoolIterator<T> iterator();
}
