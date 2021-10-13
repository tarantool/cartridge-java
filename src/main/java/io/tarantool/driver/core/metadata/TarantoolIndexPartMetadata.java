package io.tarantool.driver.core.metadata;

/**
 * Represents Tarantool index part metadata
 *
 * @author Sergey Volgin
 */
public class TarantoolIndexPartMetadata {

    private final int fieldIndex;
    private final String fieldType;
    private final String path;

    public TarantoolIndexPartMetadata(int fieldIndex, String fieldType) {
        this(fieldIndex, fieldType, null);
    }

    public TarantoolIndexPartMetadata(int fieldIndex, String fieldType, String path) {
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
        this.path = path;
    }

    /**
     * Get field index in space format
     *
     * @return field index starts with 0
     */
    public int getFieldIndex() {
        return fieldIndex;
    }

    /**
     * Get field type
     *
     * @return field type
     */
    public String getFieldType() {
        return fieldType;
    }

    /**
     * Get path inside field (for "JSON-path" indexes)
     *
     * @return path inside field (may be null)
     */
    public String getPath() {
        return path;
    }
}
