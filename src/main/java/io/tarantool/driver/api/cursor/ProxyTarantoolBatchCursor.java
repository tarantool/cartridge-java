package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.space.ProxyTarantoolSpace;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;

/**
 * @author Sergey Volgin
 */
public class ProxyTarantoolBatchCursor<T> implements TarantoolCursor<T> {

    private final ProxyTarantoolBatchCursorIterator<T> iterator;

    public ProxyTarantoolBatchCursor(
            ProxyTarantoolSpace space,
            TarantoolIndexQuery indexQuery,
            TarantoolBatchCursorOptions options,
            TarantoolCallResultMapper<T> resultMapper
    ) {
        this.iterator = new ProxyTarantoolBatchCursorIterator<>(space, indexQuery, options, resultMapper);
    }

    @Override
    public TarantoolIterator<T> iterator() {
        return iterator;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return iterator.next();
    }
}
