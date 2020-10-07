package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.space.ProxyTarantoolSpace;
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
    private final TarantoolIndexQuery indexQuery;
    private final TarantoolBatchCursorOptions options;
    private final TarantoolCallResultMapper<T> resultMapper;

    public ProxyTarantoolBatchCursorIterator(ProxyTarantoolSpace space,
                                             TarantoolIndexQuery indexQuery,
                                             TarantoolBatchCursorOptions options,
                                             TarantoolCallResultMapper<T> resultMapper) {
        this.space = space;
        this.indexQuery = indexQuery;
        this.options = options;
        this.resultMapper = resultMapper;

        this.totalPos = 0;
        this.resultPos = 0;

        getNextResult();
    }

    @Override
    public boolean hasNext() {
        return false;
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
        lastTuple = result.get(resultPos - 1);
        return lastTuple;
    }

    @SuppressWarnings("unchecked")
    protected void getNextResult() {
        TarantoolSelectOptions.Builder selectOptions = new TarantoolSelectOptions.Builder();
        selectOptions.withLimit(options.getBatchSize() + 1);

//        if (lastTuple != null) {
//
//        }

        try {
            result = (TarantoolResult<T>) space.select(indexQuery, selectOptions.build(), resultMapper).get();
            resultPos = 0;
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }
}
