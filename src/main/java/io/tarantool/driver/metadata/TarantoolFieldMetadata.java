package io.tarantool.driver.metadata;

/**
 * Tarantool space field format metadata
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public class TarantoolFieldMetadata {

    private final String fieldName;
    private final String fieldType;
    private final int fieldPosition;
    private final boolean isNullable;

    /**
     * Basic constructor.
     *
     * @param fieldName field name
     * @param fieldType field type (from the set of field types supported by the server)
     * @param fieldPosition field position in tuple starting from 0
     */
    public TarantoolFieldMetadata(String fieldName, String fieldType, int fieldPosition) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldPosition = fieldPosition;
        this.isNullable = false;
    }

    /**
     * Basic constructor with isNullable parameter.
     *
     * @param fieldName field name
     * @param fieldType field type (from the set of field types supported by the server)
     * @param fieldPosition field position in tuple starting from 0
     */
    public TarantoolFieldMetadata(String fieldName, String fieldType, int fieldPosition, boolean isNullable) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldPosition = fieldPosition;
        this.isNullable = isNullable;
    }

    /**
     * Get field name
     *
     * @return field name
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

    /**
     * Get isNullable parameter
     *
     * @return is_nullable parameter
     */
    public boolean getIsNullable() {
        return isNullable;
    }
}
