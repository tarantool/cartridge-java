package io.tarantool.driver.metadata;

/**
 * Represents Tarantool index part metadata
 *
 * @author Sergey Volgin
 */
public class TarantoolIndexPartMetadata {

    private final int fieldIndex;
    private final String fieldType;

    public TarantoolIndexPartMetadata(int fieldIndex, String fieldType) {
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
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
}
