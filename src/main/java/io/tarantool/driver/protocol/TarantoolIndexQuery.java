package io.tarantool.driver.protocol;

import org.springframework.util.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents index-related query options including index ID or name, matching keys and iterator type.
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexQuery {

    public static final int PRIMARY = 0; // Primary index has always ID 0

    private int indexId = PRIMARY;
    private TarantoolIteratorType iteratorType = TarantoolIteratorType.defaultIterator();
    private List<?> keyValues = Collections.emptyList();

    /**
     * Basic constructor. Creates a query for all tuples by primary index.
     */
    public TarantoolIndexQuery() {
    }

    /**
     * Creates a query for index with specified ID.
     * @param indexId index ID in the space
     */
    public TarantoolIndexQuery(int indexId) {
        Assert.state(indexId >= 0, "Index ID must be greater than or equal to 0");

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TarantoolIndexQuery that = (TarantoolIndexQuery) o;
        return getIndexId() == that.getIndexId() &&
                getIteratorType() == that.getIteratorType() &&
                getKeyValues().equals(that.getKeyValues());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIndexId(), getIteratorType(), getKeyValues());
    }

    @Override
    public String toString() {
        return "TarantoolIndexQuery{" +
                "indexId=" + indexId +
                ", iteratorType=" + iteratorType +
                ", keyValues=" + keyValues +
                '}';
    }
}
