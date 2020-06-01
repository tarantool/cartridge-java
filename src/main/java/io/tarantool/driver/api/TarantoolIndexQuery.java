package io.tarantool.driver.api;

import io.tarantool.driver.protocol.TarantoolIteratorType;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.List;

/**
 * Represents index-related query options including index ID or name, matching keys and iterator type.
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexQuery {

    private int indexId;
    private TarantoolIteratorType iteratorType = TarantoolIteratorType.defaultIterator();
    private List<?> keyValues = Collections.emptyList();

    /**
     * Basic constructor.
     * @param indexId index ID in the space (the primary index has ID 0)
     */
    public TarantoolIndexQuery(int indexId) {
        Assert.state(indexId > 0, "Index ID must be greater than 0");

        this.indexId = indexId;
    }

    /**
     * Get index ID
     * @return a number
     */
    public int getIndexId() {
        return indexId;
    }

    /**
     * Get iterator type
     * @return {@code TarantoolIteratorType.ITER_EQ} by default
     */
    public TarantoolIteratorType getIteratorType() {
        return iteratorType;
    }

    /**
     * Set iterator type
     * @param iteratorType new iterator type
     * @return this query instance with new iterator type
     */
    public TarantoolIndexQuery withIteratorType(TarantoolIteratorType iteratorType) {
        this.iteratorType = iteratorType;
        return this;
    }

    /**
     * Get list of key values
     * @return list of key values to be matched with index key parts
     */
    public List<?> getKeyValues() {
        return keyValues;
    }

    /**
     * Set list of key values to be matched with index key parts
     * @param keyValues new list of key values
     * @return this query instance with new list of key values
     */
    public TarantoolIndexQuery withKeyValues(List<?> keyValues) {
        Assert.notNull(keyValues, "Key values must not be null");

        this.keyValues = keyValues;
        return this;
    }
}
