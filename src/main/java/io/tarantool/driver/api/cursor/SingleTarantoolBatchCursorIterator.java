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
    private final TarantoolBatchCursorOptions options;
    private final TarantoolSimpleResultMapper<T> resultMapper;

    public SingleTarantoolBatchCursorIterator(TarantoolSpace space,
                                              TarantoolIndexQuery indexQuery,
                                              TarantoolBatchCursorOptions options,
                                              TarantoolSimpleResultMapper<T> resultMapper) {
        this.space = space;
        this.indexQuery = indexQuery;
        this.options = options;
        this.resultMapper = resultMapper;
        this.resultPos = 0;
        this.totalPos = 0;

        getNextResult();
    }

    @SuppressWarnings("unchecked")
    protected void getNextResult() {
        TarantoolSelectOptions.Builder selectOptions = new TarantoolSelectOptions.Builder();
        selectOptions.withLimit(options.getBatchSize() + 1);

        int batchStep = (int) ((totalPos + 1) / options.getBatchSize());
        if (batchStep > 0) {
            selectOptions.withOffset(batchStep * options.getBatchSize());
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
        return (resultPos < options.getBatchSize() && resultPos < result.size())
                || options.getBatchSize() < result.size();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (resultPos >= options.getBatchSize()) {
            getNextResult();
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
