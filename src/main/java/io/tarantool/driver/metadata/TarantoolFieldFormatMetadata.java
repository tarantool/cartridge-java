package io.tarantool.driver.metadata;

/**
 * Tarantool space field format metadata
 *
 * @author Sergey Volgin
 */
public class TarantoolFieldFormatMetadata {

    private final String fieldName;
    private final String fieldType;
    private final int fieldPosition;

    /**
     * Basic constructor.
     *
     * @param fieldName field name
     * @param fieldType field type (from the set of field types supported by the server)
     * @param fieldPosition field position in tuple starting from 0
     */
    public TarantoolFieldFormatMetadata(String fieldName, String fieldType, int fieldPosition) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldPosition = fieldPosition;
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

    /**
     * Get field position in space starts with 0
     *
     * @return field position in space starts with 0
     */
    public int getFieldPosition() {
        return fieldPosition;
    }
}
