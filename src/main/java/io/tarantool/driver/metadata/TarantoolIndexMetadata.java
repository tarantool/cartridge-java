package io.tarantool.driver.metadata;

/**
 * Represents Tarantool index metadata (index ID, name, etc.)
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexMetadata {

    private int spaceId;
    private int indexId;
    private String indexName;
    //TODO private TarantoolIndexType indexType
    //TODO index opts
    //TODO index parts

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

    /*
    - [289, 1, '_vindex', 'sysview', 0, {}, [{'name': 'id', 'type': 'unsigned'}, {'name': 'iid',
        'type': 'unsigned'}, {'name': 'name', 'type': 'string'}, {'name': 'type',
        'type': 'string'}, {'name': 'opts', 'type': 'map'}, {'name': 'parts', 'type': 'array'}]]
     */
}
