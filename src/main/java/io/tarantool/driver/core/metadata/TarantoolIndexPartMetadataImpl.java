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
    private final String path;

    TarantoolIndexPartMetadataImpl(int fieldIndex, String fieldType) {
        this(fieldIndex, fieldType, null);
    }

    TarantoolIndexPartMetadataImpl(int fieldIndex, String fieldType, String path) {
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
    public String getPath() {
        return path;
    }
}
