package io.tarantool.driver.api.cursor;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.api.tuple.TarantoolField;
import io.tarantool.driver.api.tuple.TarantoolNullField;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.AbstractTarantoolResultMapper;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadataOperations;
import io.tarantool.driver.protocol.TarantoolIteratorType;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * @author Sergey Volgin
 */
public class SingleTarantoolBatchCursorIterator<T> implements TarantoolIterator<T> {

    private int totalPos;
    private int resultPos;
    private T lastTuple;

    private TarantoolIteratorType queryIterator;
    private final int queryIndexId;
    private final boolean queryByPartialIndex;

    private TarantoolResult<T> result;
    private final TarantoolSpace space;
    private final Conditions initialCondition;
    private final Conditions bathCondition;
    private final TarantoolCursorOptions cursorOptions;
    private final AbstractTarantoolResultMapper<T> resultMapper;

    public SingleTarantoolBatchCursorIterator(TarantoolSpace space,
                                              Conditions initialCondition,
                                              TarantoolCursorOptions cursorOptions,
                                              AbstractTarantoolResultMapper<T> resultMapper) {
        this.space = space;
        this.initialCondition = initialCondition;
        this.cursorOptions = cursorOptions;
        this.resultMapper = resultMapper;
        this.resultPos = 0;
        this.totalPos = 0;

        this.bathCondition = createBatchCondition(initialCondition, cursorOptions);

        TarantoolIndexQuery indexQuery = initialCondition.toIndexQuery(space);
        this.queryByPartialIndex = isQueryByPartialIndex(indexQuery, space);
        this.queryIndexId = indexQuery.getIndexId();
        this.queryIterator = indexQuery.getIteratorType();

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

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
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

    private void getNextBatch() {
        int batchStep = (int) (totalPos / cursorOptions.getBatchSize()) + 1;

        if (batchStep > 1) {
            if (batchStep * cursorOptions.getBatchSize() > initialCondition.getLimit()) {
                bathCondition.withLimit(initialCondition.getLimit() - cursorOptions.getBatchSize() * (batchStep - 1));
            }

            if (queryByPartialIndex) {
                bathCondition.withOffset((batchStep - 1) * cursorOptions.getBatchSize());
            } else {
                bathCondition.clearConditions();

                queryIterator = getNextIteratorType(queryIterator);
                List<Object> indexValue = getIndexValue((TarantoolTuple) lastTuple, queryIndexId, space);
                if (queryIterator == TarantoolIteratorType.ITER_GT) {
                    bathCondition.andIndexGreaterThan(queryIndexId, indexValue);
                } else {
                    bathCondition.andIndexLessThan(queryIndexId, indexValue);
                }
            }
        }

        try {
            result = space.select(bathCondition, resultMapper).get();
            resultPos = 0;
        } catch (InterruptedException | ExecutionException e) {
            throw new TarantoolClientException(e);
        }
    }

    private TarantoolIteratorType getNextIteratorType(TarantoolIteratorType currentIteratorType) {
        TarantoolIteratorType nextIterator;
        switch (currentIteratorType) {
            case ITER_ALL:
            case ITER_EQ:
            case ITER_GE:
            case ITER_GT:
                nextIterator = TarantoolIteratorType.ITER_GT;
                break;
            case ITER_REQ:
            case ITER_LE:
            case ITER_LT:
                nextIterator = TarantoolIteratorType.ITER_LT;
                break;
            default:
                throw new TarantoolClientException("Iterator '%s' not supporter for cursor", currentIteratorType);
        }
        return nextIterator;
    }

    private boolean isQueryByPartialIndex(TarantoolIndexQuery indexQuery,
                                          TarantoolSpaceMetadataOperations operations) {
        if (indexQuery.getKeyValues().isEmpty() ||
                (indexQuery.getIteratorType() != TarantoolIteratorType.ITER_EQ &&
                        indexQuery.getIteratorType() != TarantoolIteratorType.ITER_REQ)) {
            return false;
        } else {
            TarantoolIndexMetadata indexMetadata = operations.getIndexById(indexQuery.getIndexId());
            return indexQuery.getKeyValues().size() < indexMetadata.getIndexParts().size();
        }
    }

    private List<Object> getIndexValue(TarantoolTuple tuple, int indexId, TarantoolSpaceMetadataOperations operations) {
        List<Object> value = new ArrayList<>();
        TarantoolIndexMetadata indexMetadata = operations.getIndexById(indexId);

        for (TarantoolIndexPartMetadata indexPart : indexMetadata.getIndexParts()) {
            Optional<TarantoolField> field = tuple.getField(indexPart.getFieldIndex());
            if (field.isPresent() && !(field.get() instanceof TarantoolNullField)) {
                value.add(field.get());
            } else {
                throw new TarantoolClientException("Tuple contains empty field for index value");
            }
        }
        return value;
    }
}
