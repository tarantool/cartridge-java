package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.ObjectConverter;
import io.tarantool.driver.protocol.Packable;
import org.msgpack.value.ArrayValue;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * Cursor implementation that uses 'cluster' select method
 * under the hood. Designed to work with cluster client.
 * <p>
 * See {@link TarantoolCursor} for more details on cursors.
 *
 * @author Vladimir Rogach
 */
public class StartAfterCursor<T extends Packable, R extends Collection<T>> implements TarantoolCursor<T> {

    private final TarantoolSpaceOperations<T, R> space;
    private final Conditions initConditions;

    // size of a batch for single invocation of client
    private final long batchSize;
    private long spaceOffset;

    private final ObjectConverter<T, ArrayValue> tupleConverter;

    private Iterator<T> resultIter = Collections.emptyIterator();
    private T currentValue;
    private T lastTuple;

    public StartAfterCursor(TarantoolSpaceOperations<T, R> space,
                            Conditions conditions,
                            int batchSize,
                            ObjectConverter<T, ArrayValue> tupleConverter) {
        this.space = space;
        this.initConditions = conditions;
        this.tupleConverter = tupleConverter;
        this.batchSize = batchSize;
        this.spaceOffset = 0;
    }

    /**
     * If batchSize is less than condition limit
     * we need to recalculate limit for each batch.
     *
     * @param conditionLimit initial limit (specified in condition)
     * @param batchSize      size of a batch
     * @param spaceOffset    current count of fetched elements
     * @return effective limit for the current batch
     */
    private static long calcLimit(long conditionLimit, long batchSize, long spaceOffset) {
        if (conditionLimit > 0) {
            long tuplesToFetchLeft = conditionLimit - spaceOffset;
            if (tuplesToFetchLeft < batchSize) {
                return tuplesToFetchLeft;
            }
        }
        return batchSize;
    }

    private void fetchNextTuples() throws TarantoolClientException {
        long limit = calcLimit(initConditions.getLimit(), batchSize, spaceOffset);
        if (limit <= 0) {
            return;
        }

        Conditions conditions = new Conditions(initConditions)
                .withLimit(limit);

        if (lastTuple != null) {
            conditions.startAfter(lastTuple, tupleConverter);
        }

        try {
            resultIter = space
                    .select(conditions)
                    .get()
                    .iterator();
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }

    public boolean advanceIterator() {
        if (resultIter.hasNext()) {
            currentValue = resultIter.next();
            spaceOffset += 1;
            return true;
        }
        if (currentValue != null) {
            lastTuple = currentValue;
            currentValue = null;
        }
        return false;
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
        if (currentValue == null) {
            throw new TarantoolSpaceOperationException("Can't get data in this cursor state.");
        }
        return currentValue;
    }
}
