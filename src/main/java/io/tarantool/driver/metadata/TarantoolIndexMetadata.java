package io.tarantool.driver.metadata;

import io.tarantool.driver.api.TarantoolIndexQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents Tarantool index metadata (index ID, name, etc.)
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexMetadata {

    private int spaceId;
    private int indexId;
    private String indexName;
    private TarantoolIndexType indexType;
    private TarantoolIndexOptions indexOptions;
    private List<TarantoolIndexPartMetadata> indexParts;
    private Map<Integer, TarantoolIndexPartMetadata> indexPartsByPosition;
    private Map<Integer, Integer> fieldPositionToKeyPosition;

    /**
     * Get ID of a space that this index is defined on
     * @return a number
     */
    public int getSpaceId() {
        return spaceId;
    }

    /**
     * Set space ID
     * @param spaceId a number
     */
    void setSpaceId(int spaceId) {
        this.spaceId = spaceId;
    }

    /**
     * Get index ID in the corresponding space on the Tarantool server
     * @return a natural number
     */
    public int getIndexId() {
        return indexId;
    }

    /**
     * Set index ID
     * @param indexId a positive number
     */
    void setIndexId(int indexId) {
        this.indexId = indexId;
    }

    /**
     * Get index name
     * @return a non-empty {@code String}
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Set index name
     * @param indexName a non-empty {@code String}
     */
    void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Get index type
     * @return the index type
     */
    public TarantoolIndexType getIndexType() {
        return indexType;
    }

    /**
     * Set index type
     * @param indexType a non-empty {@link TarantoolIndexType}
     */
    public void setIndexType(TarantoolIndexType indexType) {
        this.indexType = indexType;
    }

    /**
     * Get index options
     * @return index options
     */
    public TarantoolIndexOptions getIndexOptions() {
        return indexOptions;
    }

    /**
     * Set index options
     * @param indexOptions a not-empty {@link TarantoolIndexOptions}
     */
    public void setIndexOptions(TarantoolIndexOptions indexOptions) {
        this.indexOptions = indexOptions;
    }

    /**
     * Set index parts
     * @param indexParts a not-empty list of {@link TarantoolIndexPartMetadata}
     */
    public void setIndexParts(List<TarantoolIndexPartMetadata> indexParts) {
        this.indexParts = indexParts;
        this.indexPartsByPosition = indexParts.stream()
                .collect(Collectors.toMap(TarantoolIndexPartMetadata::getFieldIndex, Function.identity()));
        this.fieldPositionToKeyPosition = new HashMap<>();
        int index = 0;
        for (TarantoolIndexPartMetadata meta : indexParts) {
            fieldPositionToKeyPosition.put(index++, meta.getFieldIndex());
        }
    }

    /**
     * Get index parts
     * @return a not-empty list of {@link TarantoolIndexPartMetadata}
     */
    public List<TarantoolIndexPartMetadata> getIndexParts() {
        return indexParts;
    }

    /**
     * Get index parts by field indexes
     * @return a not-empty map of index positions to {@link TarantoolIndexPartMetadata}
     */
    public Map<Integer, TarantoolIndexPartMetadata> getIndexPartsByPosition() {
        return indexPartsByPosition;
    }

    /**
     * Get map of field positions to index parts positions
     * @return field position
     */
    public Integer getIndexPartPositionByFieldPosition(int fieldPosition) {
        return fieldPositionToKeyPosition.get(fieldPosition);
    }

    /**
     * Returns true if this is a primary index, false otherwise.
     * @return true if this is a primary index, false otherwise.
     */
    public boolean isPrimary() {
        return indexId == TarantoolIndexQuery.PRIMARY;
    }

    /**
     * Returns true if this is a unique index, false otherwise.
     * @return true if this is a unique index, false otherwise.
     */
    public boolean isUnique() {
        return isPrimary() || indexOptions.isUnique();
    }
}
