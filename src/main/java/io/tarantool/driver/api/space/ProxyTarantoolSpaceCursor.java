package io.tarantool.driver.api.space;

import io.tarantool.driver.api.TarantoolCursor;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.ObjectConverter;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.concurrent.ExecutionException;

/**
 * Cursor implementation that uses 'cluster' select method
 * under the hood. Designed to work with cluster client.
 *
 * See {@link TarantoolCursor} for more details on cursors.
 *
 * @author Vladimir Rogach
 */
public class ProxyTarantoolSpaceCursor<T> implements TarantoolCursor<T> {

    private final ProxyTarantoolSpace space;
    private final ValueConverter<ArrayValue, T> valueConverter;
    private final ObjectConverter<T, ArrayValue> tupleConverter;
    private final Conditions initConditions;

    // size of a batch for single invocation of client
    private final long batchSize;
    // current offset in batch
    private int batchOffset;

    private TarantoolResult<T> result;

    public ProxyTarantoolSpaceCursor(ProxyTarantoolSpace space,
                                     Conditions conditions,
                                     int batchSize,
                                     ValueConverter<ArrayValue, T> valueConverter,
                                     ObjectConverter<T, ArrayValue> tupleConverter) {
        this.space = space;
        this.initConditions = conditions;
        this.valueConverter = valueConverter;
        this.tupleConverter = tupleConverter;
        this.batchOffset = 0;

        if (conditions.getLimit() > 0 && conditions.getLimit() < batchSize) {
            this.batchSize = conditions.getLimit();
        } else {
            this.batchSize = batchSize;
        }
    }

    private void fetchNextTuples() throws TarantoolClientException {
        Conditions conditions = new Conditions(initConditions)
                .withLimit(batchSize);

        if (result != null && result.size() > 0) {
            T lastTuple = result.get(result.size() - 1);
            conditions.startAfter(lastTuple, tupleConverter);
        }

        try {
            result = space.select(conditions, valueConverter).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public boolean next() throws TarantoolClientException {

        if (result.size() > batchOffset + 1) {
            batchOffset += 1;
            return true;
        }

        fetchNextTuples();
        batchOffset = 0;

        return result.size() > 0;
    }

    @Override
    public T get() throws TarantoolException {
        if (result.size() <= batchOffset - 1) {
            throw new TarantoolSpaceOperationException("Batch does not contain elements.");
        }
        return result.get(batchOffset);
    }

}
