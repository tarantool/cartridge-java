package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * @author Sergey Volgin
 */
public class SingleTarantoolBatchCursor<T> implements TarantoolCursor<T> {

    private final TarantoolIterator<T> iterator;

    public SingleTarantoolBatchCursor(TarantoolSpace space,
                                      TarantoolIndexQuery indexQuery,
                                      TarantoolBatchCursorOptions options,
                                      ValueConverter<ArrayValue, T> tupleMapper) {
        this.iterator = new SingleTarantoolBatchCursorIterator<>(space, indexQuery, options, tupleMapper);
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
