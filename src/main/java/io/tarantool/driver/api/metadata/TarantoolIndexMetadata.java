package io.tarantool.driver.api.metadata;


import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents Tarantool index metadata (index ID, name, etc.)
 *
 * @author Alexey Kuzin
 */
public interface TarantoolIndexMetadata {
    /**
     * Get ID of a space that this index is defined on
     *
     * @return a number
     */
    int getSpaceId();

    /**
     * Get index ID in the corresponding space on the Tarantool server
     *
     * @return a natural number
     */
    int getIndexId();

    /**
     * Get index name
     *
     * @return a non-empty {@code String}
     */
    String getIndexName();

    /**
     * Get index type
     *
     * @return the index type
     */
    TarantoolIndexType getIndexType();

    /**
     * Set index type
     *
     * @param indexType a non-empty {@link TarantoolIndexType}
     */
    void setIndexType(TarantoolIndexType indexType);

    /**
     * Get index options
     *
     * @return index options
     */
    TarantoolIndexOptions getIndexOptions();

    /**
     * Set index options
     *
     * @param indexOptions a not-empty {@link TarantoolIndexOptions}
     */
    void setIndexOptions(TarantoolIndexOptions indexOptions);

    /**
     * Set index parts
     *
     * @param indexParts a not-empty list of {@link TarantoolIndexPartMetadata}
     */
    void setIndexParts(List<TarantoolIndexPartMetadata> indexParts);

    /**
     * Get index parts
     *
     * @return a not-empty list of {@link TarantoolIndexPartMetadata}
     */
    List<TarantoolIndexPartMetadata> getIndexParts();

    /**
     * Get index parts by field indexes
     *
     * @return a not-empty map of index positions to {@link TarantoolIndexPartMetadata}
     */
    Map<Integer, TarantoolIndexPartMetadata> getIndexPartsByPosition();

    /**
     * Get map of field positions to index parts positions
     *
     * @param fieldPosition field position in tuple, starting from 0
     * @return field position
     */
    Optional<Integer> getIndexPartPositionByFieldPosition(int fieldPosition);

    /**
     * Returns true if this is a primary index, false otherwise.
     *
     * @return true if this is a primary index, false otherwise.
     */
    boolean isPrimary();

    /**
     * Returns true if this is a unique index, false otherwise.
     *
     * @return true if this is a unique index, false otherwise.
     */
    boolean isUnique();
}
