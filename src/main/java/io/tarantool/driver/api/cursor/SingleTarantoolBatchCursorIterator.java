package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.TarantoolSimpleResultMapper;

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
    private final TarantoolIndexQuery indexQuery;
    private final TarantoolBatchCursorOptions cursorOptions;
    private final TarantoolSimpleResultMapper<T> resultMapper;

    public SingleTarantoolBatchCursorIterator(TarantoolSpace space,
                                              TarantoolIndexQuery indexQuery,
                                              TarantoolBatchCursorOptions cursorOptions,
                                              TarantoolSimpleResultMapper<T> resultMapper) {
        this.space = space;
        this.indexQuery = indexQuery;
        this.cursorOptions = cursorOptions;
        this.resultMapper = resultMapper;
        this.resultPos = 0;
        this.totalPos = 0;

        getNextBatch();
    }

    @SuppressWarnings("unchecked")
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
            result = (TarantoolResult<T>) space.select(indexQuery, selectOptions.build(), resultMapper).get();
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
