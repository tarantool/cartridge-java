package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.mappers.AbstractTarantoolResultMapper;

/**
 * @author Sergey Volgin
 */
public class SingleTarantoolBatchCursor<T> implements TarantoolCursor<T> {

    private final TarantoolIterator<T> iterator;

    public SingleTarantoolBatchCursor(TarantoolSpace space,
                                      Conditions conditions,
                                      TarantoolCursorOptions options,
                                      AbstractTarantoolResultMapper<T> resultMapper) {
        this.iterator = new SingleTarantoolBatchCursorIterator<>(space, conditions, options, resultMapper);
    }

    @Override
    public void close() {
    }

    @Override
    public TarantoolIterator<T> iterator() {
        return iterator;
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
