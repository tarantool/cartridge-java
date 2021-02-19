package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * Generic cursor imlementation that performs client calls
 * with TarantoolSpaceOperations.
 *
 * @author Vladimir Rogach
 */
public abstract class TarantoolCursorBase<T extends Packable, R extends Collection<T>>
        implements TarantoolCursor<T> {

    abstract protected void fetchNextTuples();
    abstract protected boolean advanceIterator();
    abstract protected T getCurrentValue();

    /**
     * If batchSize is less than condition limit
     * we need to recalculate limit for each batch.
     *
     * @param conditionLimit initial limit (specified in condition)
     * @param batchSize      size of a batch
     * @param spaceOffset    current count of fetched elements
     * @return effective limit for the current batch
     */
    protected static long calcLimit(long conditionLimit, long batchSize, long spaceOffset) {
        if (conditionLimit > 0) {
            long tuplesToFetchLeft = conditionLimit - spaceOffset;
            if (tuplesToFetchLeft < batchSize) {
                return tuplesToFetchLeft;
            }
        }
        return batchSize;
    }

    @Override
    public boolean next() throws TarantoolClientException {
        if (!advanceIterator()) {
            fetchNextTuples();
            return advanceIterator();
        }
        return true;
    }

    @Override
    public T get() throws TarantoolSpaceOperationException {
        if (getCurrentValue() == null) {
            throw new TarantoolSpaceOperationException(
                    "Unexpected cursor state: check that next() is called");
        }
        return getCurrentValue();
    }
}
