package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

/**
 * Cursor implementation that uses server 'limit' and 'offset'
 * methods under the hood.
 * <p>
 * Note: it is possible to use this class with standalone server only.
 * <p>
 * See {@link TarantoolCursor} for more details on cursors.
 *
 * @author Vladimir Rogach
 */
public class OffsetCursor<T extends Packable, R extends Collection<T>> extends TarantoolCursorBase<T, R> {

    private final TarantoolSpaceOperations<T, R> space;
    private final Conditions initConditions;

    // size of a batch for single invocation of client
    private final long batchSize;
    private long spaceOffset;

    private Iterator<T> resultIter = Collections.emptyIterator();
    private T currentValue;

    public OffsetCursor(
        TarantoolSpaceOperations<T, R> space,
        Conditions conditions,
        int batchSize) {
        this.space = space;
        this.initConditions = conditions;
        this.spaceOffset = 0;
        this.batchSize = batchSize;
    }

    /**
     * Perform a call o server for fetching the next batch.
     *
     * @throws TarantoolClientException if select query was interrupted by client.
     */
    @Override
    protected void fetchNextTuples() throws TarantoolClientException {
        long limit = calcLimit(initConditions.getLimit(), batchSize, spaceOffset);

        if (limit <= 0) {
            return;
        }

        Conditions conditions = new Conditions(initConditions)
            .withLimit(limit)
            .withOffset(spaceOffset);

        try {
            resultIter = space
                .select(conditions)
                .get()
                .iterator();
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    protected boolean advanceIterator() {
        if (resultIter.hasNext()) {
            currentValue = resultIter.next();
            spaceOffset += 1;
            return true;
        }
        currentValue = null;
        return false;
    }

    @Override
    protected T getCurrentValue() {
        return currentValue;
    }

}
