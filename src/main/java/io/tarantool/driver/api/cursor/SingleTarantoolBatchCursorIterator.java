package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.AbstractTarantoolResultMapper;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

/**
 * @author Sergey Volgin
 */
public class SingleTarantoolBatchCursorIterator<T> implements TarantoolIterator<T> {

    private int totalPos;
    private int resultPos;
    private TarantoolResult<T> result;
    private final TarantoolSpace space;
    private final Conditions conditions;
    private final TarantoolCursorOptions cursorOptions;
    private final AbstractTarantoolResultMapper<T> resultMapper;

    public SingleTarantoolBatchCursorIterator(TarantoolSpace space,
                                              Conditions conditions,
                                              TarantoolCursorOptions cursorOptions,
                                              AbstractTarantoolResultMapper<T> resultMapper) {
        this.space = space;
        this.conditions = conditions;
        this.cursorOptions = cursorOptions;
        this.resultMapper = resultMapper;
        this.resultPos = 0;
        this.totalPos = 0;

        getNextBatch();
    }

    protected void getNextBatch() {
        TarantoolSelectOptions.Builder selectOptions = new TarantoolSelectOptions.Builder();
        selectOptions.withLimit(cursorOptions.getBatchSize());

        int batchStep = (int) ((totalPos + 1) / cursorOptions.getBatchSize());
        if (cursorOptions.getBatchSize() == 1) {
            batchStep = totalPos;
        }
        if (batchStep > 0) {
            selectOptions.withOffset(batchStep * cursorOptions.getBatchSize());
        }

        try {
            result = space.select(conditions, resultMapper).get();
            resultPos = 0;
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
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
        return result.get(resultPos - 1);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
