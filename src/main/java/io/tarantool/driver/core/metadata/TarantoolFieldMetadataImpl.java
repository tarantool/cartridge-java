package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;

/**
 * Tarantool space field format metadata
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
class TarantoolFieldMetadataImpl implements TarantoolFieldMetadata {

    private static final long serialVersionUID = 20200708L;

    private final String fieldName;
    private final String fieldType;
    private final int fieldPosition;
    private final boolean isNullable;

    /**
     * Basic constructor.
     *
     * @param fieldName     field name
     * @param fieldType     field type (from the set of field types supported by the server)
     * @param fieldPosition field position in tuple starting from 0
     */
    TarantoolFieldMetadataImpl(String fieldName, String fieldType, int fieldPosition) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldPosition = fieldPosition;
        this.isNullable = false;
    }

    /**
     * Basic constructor with isNullable parameter.
     *
     * @param fieldName     field name
     * @param fieldType     field type (from the set of field types supported by the server)
     * @param fieldPosition field position in tuple starting from 0
     * @param isNullable    is field nullable
     */
    TarantoolFieldMetadataImpl(String fieldName, String fieldType, int fieldPosition, boolean isNullable) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.fieldPosition = fieldPosition;
        this.isNullable = isNullable;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getFieldType() {
        return fieldType;
    }

    @Override
    public int getFieldPosition() {
        return fieldPosition;
    }

    @Override
    public boolean getIsNullable() {
        return isNullable;
    }
}
