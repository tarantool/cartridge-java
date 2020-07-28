package io.tarantool.driver.metadata;

/**
 * Tarantool space field format metadata
 *
 */
public class TarantoolFieldFormatMetadata {

    private final String fieldName;
    private final String fieldType;

    /**
     * Basic constructor.
     */
    public TarantoolFieldFormatMetadata(String fieldName, String fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    /**
     * Get field name
     *
     * @return fiend name
     */
    public String getFieldName() {
        return fieldName;
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
