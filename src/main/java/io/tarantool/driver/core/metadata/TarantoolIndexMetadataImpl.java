package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexOptions;
import io.tarantool.driver.api.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexType;
import io.tarantool.driver.protocol.TarantoolIndexQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents Tarantool index metadata (index ID, name, etc.)
 *
 * @author Alexey Kuzin
 */
class TarantoolIndexMetadataImpl implements TarantoolIndexMetadata {

    private int spaceId;
    private int indexId;
    private String indexName;
    private TarantoolIndexType indexType;
    private TarantoolIndexOptions indexOptions;
    private List<TarantoolIndexPartMetadata> indexParts;
    private Map<Integer, TarantoolIndexPartMetadata> indexPartsByPosition;
    private Map<Integer, Integer> fieldPositionToKeyPosition;

    @Override
    public int getSpaceId() {
        return spaceId;
    }

    /**
     * Set space ID
     *
     * @param spaceId a number
     */
    void setSpaceId(int spaceId) {
        this.spaceId = spaceId;
    }

    @Override
    public int getIndexId() {
        return indexId;
    }

    /**
     * Set index ID
     *
     * @param indexId a positive number
     */
    void setIndexId(int indexId) {
        this.indexId = indexId;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    /**
     * Set index name
     *
     * @param indexName a non-empty {@code String}
     */
    void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public TarantoolIndexType getIndexType() {
        return indexType;
    }

    @Override
    public void setIndexType(TarantoolIndexType indexType) {
        this.indexType = indexType;
    }

    @Override
    public TarantoolIndexOptions getIndexOptions() {
        return indexOptions;
    }

    @Override
    public void setIndexOptions(TarantoolIndexOptions indexOptions) {
        this.indexOptions = indexOptions;
    }

    @Override
    public void setIndexParts(List<TarantoolIndexPartMetadata> indexParts) {
        this.indexParts = indexParts;
        this.indexPartsByPosition = indexParts.stream()
            .collect(Collectors.toMap(TarantoolIndexPartMetadata::getFieldIndex, Function.identity()));
        this.fieldPositionToKeyPosition = new HashMap<>();
        int index = 0;
        for (TarantoolIndexPartMetadata meta : indexParts) {
            fieldPositionToKeyPosition.put(meta.getFieldIndex(), index++);
        }
    }

    @Override
    public List<TarantoolIndexPartMetadata> getIndexParts() {
        return indexParts;
    }

    @Override
    public Map<Integer, TarantoolIndexPartMetadata> getIndexPartsByPosition() {
        return indexPartsByPosition;
    }

    @Override
    public Optional<Integer> getIndexPartPositionByFieldPosition(int fieldPosition) {
        return Optional.ofNullable(fieldPositionToKeyPosition.get(fieldPosition));
    }

    @Override
    public boolean isPrimary() {
        return indexId == TarantoolIndexQuery.PRIMARY;
    }

    @Override
    public boolean isUnique() {
        return isPrimary() || indexOptions.isUnique();
    }
}
