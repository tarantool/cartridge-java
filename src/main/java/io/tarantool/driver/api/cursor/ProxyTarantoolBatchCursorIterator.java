package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.ProxyTarantoolSpace;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.metadata.TarantoolFieldFormatMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

import java.util.LinkedHashMap;
import java.util.Map;
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
    private final TarantoolSpaceMetadata spaceMetadata;
    private final Conditions conditions;
    private final TarantoolCursorOptions cursorOptions;
    private final TarantoolCallResultMapper<T> resultMapper;

    public ProxyTarantoolBatchCursorIterator(ProxyTarantoolSpace space,
                                             Conditions conditions,
                                             TarantoolCursorOptions cursorOptions,
                                             TarantoolCallResultMapper<T> resultMapper,
                                             TarantoolSpaceMetadata spaceMetadata) {
        this.space = space;
        this.spaceMetadata = spaceMetadata;
        this.conditions = conditions;
        this.cursorOptions = cursorOptions;
        this.resultMapper = resultMapper;

        this.totalPos = 0;
        this.resultPos = 0;

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
        TarantoolSelectOptions.Builder selectOptions = new TarantoolSelectOptions.Builder();
        selectOptions.withLimit(cursorOptions.getBatchSize());

        conditions.withLimit(cursorOptions.getBatchSize());

//        if (lastTuple != null) {
//            selectOptions.withAfter(tupleToMap((TarantoolTuple) lastTuple));
//        }

        try {
            result = space.select(conditions, resultMapper).get();
            resultPos = 0;
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }
}
