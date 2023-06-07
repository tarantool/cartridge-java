package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolIndexPartMetadata;

/**
 * Represents Tarantool index part metadata
 *
 * @author Sergey Volgin
 */
class TarantoolIndexPartMetadataImpl implements TarantoolIndexPartMetadata {

    private final int fieldIndex;
    private final String fieldType;
    private final Object path;

    TarantoolIndexPartMetadataImpl(int fieldIndex, String fieldType) {
        this(fieldIndex, fieldType, null);
    }

    TarantoolIndexPartMetadataImpl(int fieldIndex, String fieldType, Object path) {
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
        this.path = path;
    }

    @Override
    public int getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public String getFieldType() {
        return fieldType;
    }

    @Override
    public Object getPath() {
        return path;
    }
}
