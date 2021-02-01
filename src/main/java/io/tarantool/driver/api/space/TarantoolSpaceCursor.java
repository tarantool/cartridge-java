package io.tarantool.driver.api.space;

import io.tarantool.driver.api.TarantoolCursor;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;

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
public class TarantoolSpaceCursor<T> implements TarantoolCursor<T> {

    private final TarantoolSpace space;
    private final Class<T> tupleClass;
    private final Conditions initConditions;

    // size of a batch for single invocation of client
    private final long batchSize;
    // current offset in batch
    private int batchOffset;
    // global offset in space
    private long spaceOffset;

    private TarantoolResult<T> result;

    public TarantoolSpaceCursor(TarantoolSpace space,
                                Conditions conditions,
                                int batchSize,
                                Class<T> tupleClass) {
        this.space = space;
        this.initConditions = conditions;
        this.tupleClass = tupleClass;
        this.spaceOffset = 0;
        this.batchOffset = 0;
        this.batchSize = batchSize;
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

    /**
     * Perform a call o server to fetch next batch.
     *
     * @throws TarantoolClientException if select query was interrupted by client.
     */
    private void fetchNextTuples() throws TarantoolClientException {
        long limit = calcLimit(initConditions.getLimit(), batchSize, spaceOffset);
        Conditions conditions = new Conditions(initConditions)
                .withLimit(limit)
                .withOffset(spaceOffset);

        try {
            result = space.select(conditions, tupleClass).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }

        spaceOffset = spaceOffset + result.size();
    }

    @Override
    public boolean next() throws TarantoolClientException {

        if (result != null && result.size() > batchOffset + 1) {
            batchOffset += 1;
            return true;
        }

        fetchNextTuples();
        batchOffset = 0;

        return result.size() > 0;
    }

    @Override
    public T get() throws TarantoolException {
        if (result != null && result.size() > batchOffset) {
            return result.get(batchOffset);
        }
        throw new TarantoolSpaceOperationException("Batch does not contain elements.");
    }

}
