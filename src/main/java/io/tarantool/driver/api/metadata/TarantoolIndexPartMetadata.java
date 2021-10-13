package io.tarantool.driver.api.metadata;

/**
 * Represents Tarantool index part metadata
 *
 * @author Sergey Volgin
 */
public interface TarantoolIndexPartMetadata {
    /**
     * Get field index in space format
     *
     * @return field index starts with 0
     */
    int getFieldIndex();

    /**
     * Get field type
     *
     * @return field type
     */
    String getFieldType();

    /**
     * Get path inside field (for "JSON-path" indexes)
     *
     * @return path inside field (may be null)
     */
    String getPath();
}
