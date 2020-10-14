package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.ProxyTarantoolSpace;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

/**
 * @author Sergey Volgin
 */
public class ProxyTarantoolBatchCursorIterator<T> implements TarantoolIterator<T> {

    private int totalPos;
    private int resultPos;
    private TarantoolResult<T> result;
    private T lastTuple;

    private final ProxyTarantoolSpace space;
    private final Conditions initialCondition;
    private final Conditions bathCondition;
    private final TarantoolCursorOptions cursorOptions;
    private final TarantoolCallResultMapper<T> resultMapper;

    public ProxyTarantoolBatchCursorIterator(ProxyTarantoolSpace space,
                                             Conditions initialCondition,
                                             TarantoolCursorOptions cursorOptions,
                                             TarantoolCallResultMapper<T> resultMapper) {
        this.space = space;
        this.initialCondition = initialCondition;
        this.cursorOptions = cursorOptions;
        this.resultMapper = resultMapper;
        this.totalPos = 0;
        this.resultPos = 0;

        this.bathCondition = createBatchCondition(initialCondition, cursorOptions);

        getNextBatch();
    }

    @Override
    public boolean hasNext() {
        if (result.size() == cursorOptions.getBatchSize() && resultPos + 1 > cursorOptions.getBatchSize()) {
            getNextBatch();
        }

        return result.size() > 0 && resultPos < cursorOptions.getBatchSize() && resultPos < result.size();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        resultPos++;
        totalPos++;
        lastTuple = result.get(resultPos - 1);
        return lastTuple;
    }

    protected void getNextBatch() {
        int batchStep = (int) (totalPos / cursorOptions.getBatchSize()) + 1;

        if (lastTuple != null) {
            bathCondition.startAfter((TarantoolTuple) lastTuple);
        }

        if (batchStep > 1) {
            if (batchStep * cursorOptions.getBatchSize() > initialCondition.getLimit()) {
                bathCondition.withLimit(initialCondition.getLimit() - cursorOptions.getBatchSize() * (batchStep - 1));
            }
        }

        try {
            result = space.select(bathCondition, resultMapper).get();
            resultPos = 0;
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }

    private Conditions createBatchCondition(Conditions source, TarantoolCursorOptions cursorOptions) {
        Conditions conditions = Conditions.clone(source);
        if (source.getLimit() < cursorOptions.getBatchSize()) {
            conditions.withLimit(source.getLimit());
        } else {
            conditions.withLimit(cursorOptions.getBatchSize());
        }
        return conditions;
    }

}
