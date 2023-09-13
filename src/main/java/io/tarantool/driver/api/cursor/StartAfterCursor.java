package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.options.interfaces.TarantoolSpaceOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.protocol.Packable;

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
public class StartAfterCursor<T extends Packable, R extends Collection<T>> extends TarantoolCursorBase<T, R> {

    private final TarantoolSpaceOperations<T, R> space;
    private final Conditions initConditions;

    // size of a batch for single invocation of client
    private final long batchSize;
    private long spaceOffset;

    private final MessagePackMapper mapper;

    private Iterator<T> resultIter = Collections.emptyIterator();
    private T currentValue;
    private T lastTuple;

    public StartAfterCursor(
        TarantoolSpaceOperations<T, R> space,
        Conditions conditions,
        int batchSize,
        MessagePackMapper mapper) {
        this.space = space;
        this.initConditions = conditions;
        this.batchSize = batchSize;
        this.spaceOffset = 0;
        this.mapper = mapper;
    }

    @Override
    protected void fetchNextTuples() throws TarantoolClientException {
        long limit = calcLimit(initConditions.getLimit(), batchSize, spaceOffset);
        if (limit <= 0) {
            return;
        }

        Conditions conditions = new Conditions(initConditions)
            .withLimit(limit);

        if (lastTuple != null) {
            conditions.startAfter(lastTuple, mapper::toValue);
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

    @Override
    protected boolean advanceIterator() {
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
    protected T getCurrentValue() {
        return currentValue;
    }

}
